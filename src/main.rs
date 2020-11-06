use futures::stream::{FuturesOrdered, StreamExt};
use std::collections::HashMap;
use std::error::Error;
use std::result::Result;
use std::string::String;
use std::sync::Arc;
use tokio;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::RwLock;

use std::fmt;
use tokio_postgres::{Client, NoTls, Statement};
use Command::*;

pub struct Execer<S: PgStatement> {
    tx_commands: Option<mpsc::Sender<Command<S>>>,
    done_rx: oneshot::Receiver<tokio::task::JoinHandle<()>>,
    res: Arc<RwLock<Option<Result<(), MyError>>>>,
}

impl<S: 'static + PgStatement> Execer<S> {
    pub fn new(c: Client) -> Self {
        let result = Arc::new(RwLock::new(None));

        let (commands_tx, commands_rx) = mpsc::channel(1024);
        let (done_tx, done_rx) = oneshot::channel();

        let execer = Execer {
            tx_commands: Some(commands_tx),
            done_rx,
            res: result.clone(),
        };

        let jh: tokio::task::JoinHandle<()> = tokio::spawn(async move {
            let mut rx = commands_rx;

            let ret = Execer::mainloop(c, &mut rx).await;

            let mut x = result.write().await;
            println!("writing to result : {:?}", &ret);
            *x = Some(ret);
        });

        done_tx.send(jh).expect("BUG: failed to send done join handle");

        execer
    }

    async fn mainloop(mut c: Client, commands_rx: &mut mpsc::Receiver<Command<S>>) -> Result<(), MyError> {
        let mut prepared: HashMap<String, Statement> = HashMap::new();

        let c1 = &mut c;

        let mut done = false;
        while !done {
            let mut futures = FuturesOrdered::new();

            let mut commit = false;
            let tx = Arc::new(RwLock::new(Some(c1.transaction().await?)));

            while (!done && !commit) || futures.len() > 0 {
                tokio::select!(
                    val = commands_rx.recv(), if !done && !commit && futures.len() < 8192 => {
                        match val  {
                            None => { done = true; }
                            Some(cmd) =>{
                                match cmd {
                                    Statement(s) => {
                                        let st : Statement;
                                        if let Some(s) = prepared.get(&s.statement_string().to_string()) {
                                            st = s.to_owned();
                                        } else {
                                            // we need to flush the futures here, otherwise the following prepare can block.
                                            while futures.len() > 0 {
                                                futures.next().await.unwrap()?;
                                            }

                                            let new_statement = tx.read().await.as_ref().unwrap().prepare(&s.statement_string()).await?.clone();
                                            prepared.insert(s.statement_string().to_string(), new_statement.clone());
                                            st = new_statement;
                                        }

                                        let tx1  =tx.clone();
                                        futures.push(async move {
                                            let lg = tx1.read().await;
                                            let tx = lg.as_ref().unwrap();
                                            match tx.execute(&st, &s.params()).await {
                                                Ok(_) => Ok(()),
                                                Err(e) => {
                                                    Err(MyError::from(format!("{} : failed query {:?} with params {:?}", e, &s.statement_string(), &s.params())))
                                                }
                                            }
                                        });
                                    }
                                    Flush =>{
                                        while futures.len() > 0 {
                                            futures.next().await.unwrap()?;
                                        //    count +=1;
                                        }
                                    }
                                    Commit => {
                                        commit = true;
                                    }
                                }
                            }
                        }
                    }
                    val = futures.next(), if !futures.is_empty() => {
                            val.unwrap()?;
                    }

                );
            }
            if commit {
                let the_tx = tx.clone().write().await.take();
                match the_tx {
                    None => panic!("bug."),
                    Some(x) => {
                        x.commit().await?;
                    }
                }
            }
        }

        Ok(())
    }

    async fn submit(&mut self, cmd: Command<S>) -> Result<(), MyError> {
        match &mut self.tx_commands {
            None => Err(MyError::from("Closed")),
            Some(tx_commands) => match tx_commands.send(cmd).await {
                Err(_e) => self.res.read().await.clone().expect("bug"),
                Ok(()) => Ok(()),
            },
        }
    }

    pub async fn execute(&mut self, stmt: S) -> Result<(), MyError> {
        self.submit(Command::Statement(stmt)).await
    }

    pub async fn flush(&mut self) -> Result<(), MyError> {
        self.submit(Command::Commit).await
    }

    pub async fn commit(&mut self) -> Result<(), MyError> {
        self.submit(Command::Commit).await
    }

    pub async fn close(&mut self) -> std::result::Result<(), MyError> {
        self.tx_commands = None; // cause Sender to be dropped.
        self.done_rx.try_recv()?.await?;
        self.res.read().await.as_ref().expect("bug in Execer").clone()
    }
}

pub trait PgStatement: Send + Sync {
    fn statement_string(&self) -> &str;
    fn params(&self) -> Vec<&(dyn tokio_postgres::types::ToSql + Sync)>;
}

enum Command<S: PgStatement> {
    Statement(S),
    Flush,
    Commit,
}

#[derive(Debug)]
pub struct Stmt {
    pub s: Arc<String>,
    pub(crate) params: VecParams,
}

#[derive(Debug)]
pub(crate) struct VecParams {
    pub params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>>,
}

impl VecParams {
    fn params(&self) -> Vec<&(dyn tokio_postgres::types::ToSql + Sync)> {
        self.params
            .iter()
            .map(|s| s.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect::<Vec<_>>()
    }
}

pub(crate) struct AnyParamIter<'a> {
    params: std::vec::IntoIter<&'a (dyn tokio_postgres::types::ToSql + Sync)>,
}

impl<'a> Iterator for AnyParamIter<'a> {
    type Item = &'a (dyn tokio_postgres::types::ToSql + Sync);

    fn next(&mut self) -> Option<Self::Item> {
        self.params.next()
    }
}

impl<'a> IntoIterator for &'a VecParams {
    type Item = &'a (dyn tokio_postgres::types::ToSql + Sync);
    type IntoIter = AnyParamIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        AnyParamIter {
            params: self.params().into_iter(),
        }
    }
}

impl PgStatement for Stmt {
    fn statement_string(&self) -> &str {
        &self.s
    }

    fn params(&self) -> Vec<&(dyn tokio_postgres::types::ToSql + Sync)> {
        self.params.params()
    }
}

#[tokio::main]
async fn main() -> Result<(), MyError> {
    let (client, connection) = tokio_postgres::connect("host=localhost user=postgres password=postgres", NoTls).await?;
    tokio::task::spawn(async {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let mut execer = Execer::new(client);

    execer
        .execute(Stmt {
            s: Arc::new(
                "create table if not exists test01 (id bigint,created_at TIMESTAMPTZ DEFAULT (Now() at time zone 'utc'));"
                    .to_string(),
            ),
            params: VecParams { params: vec![] },
        })
        .await?;

    execer.flush().await?;

    for i in 1_i64..1000000 {
        let st = Stmt {
            s: Arc::new("insert into test01 values($1)".to_string()),
            params: VecParams {
                params: vec![Box::new(i)],
            },
        };
        execer.execute(st).await?;
    }

    execer.commit().await?;

    execer.close().await?;

    println!("success");

    Ok(())
}
#[derive(Debug, Clone)]
pub struct MyError {
    s: String,
}
impl std::convert::From<tokio::task::JoinError> for MyError {
    fn from(e: tokio::task::JoinError) -> Self {
        MyError {
            s: String::from(e.to_string()),
        }
    }
}
impl std::convert::From<String> for MyError {
    fn from(e: String) -> Self {
        MyError { s: e }
    }
}
impl std::convert::From<tokio::sync::oneshot::error::TryRecvError> for MyError {
    fn from(e: oneshot::error::TryRecvError) -> Self {
        MyError {
            s: String::from(e.to_string()),
        }
    }
}
impl<T> std::convert::From<tokio::sync::mpsc::error::SendError<T>> for MyError {
    fn from(e: mpsc::error::SendError<T>) -> Self {
        MyError {
            s: String::from(e.to_string()),
        }
    }
}
impl std::convert::From<tokio_postgres::error::Error> for MyError {
    fn from(e: tokio_postgres::error::Error) -> Self {
        MyError {
            s: String::from(e.to_string()),
        }
    }
}
impl std::convert::From<&str> for MyError {
    fn from(e: &str) -> Self {
        MyError {
            s: String::from(e.to_string()),
        }
    }
}
impl fmt::Display for MyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.s)
    }
}
impl Error for MyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}
