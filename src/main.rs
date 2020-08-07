//#![feature(type_ascription)]
use futures::stream::{FuturesOrdered, StreamExt};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::result::Result;
use std::string::String;
use std::sync::Arc;
use tokio;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::RwLock;

use tokio_postgres::{Client, NoTls, Statement};
use Command::*;
struct Execer {
    tx_commands: Option<mpsc::Sender<Command>>,
    done_rx: oneshot::Receiver<tokio::task::JoinHandle<std::result::Result<(), MyError>>>,
}

impl Execer {
    fn new(mut c: Client) -> Self {
        let (commands_tx, mut commands_rx) = mpsc::channel(1024);
        let (done_tx, done_rx) = oneshot::channel();

        let jh: tokio::task::JoinHandle<std::result::Result<(), MyError>> = tokio::spawn(async move {
            let mut prepared: HashMap<String, Statement> = HashMap::new();

            let c1 = &mut c;

            let tx = Arc::new(RwLock::new(Some(c1.transaction().await?)));

            let mut futures = FuturesOrdered::new();
            let mut done = false;
            while !done || futures.len() > 0 {
                tokio::select!(
                    val = commands_rx.recv(), if !done && futures.len() < 8192 => {
                        match val  {
                            None => { done = true; }
                            Some(cmd) =>{
                                match cmd {
                                    Statement(s) => {
                                        let st : Statement;
                                        if let Some(s) = prepared.get(&s.s) {
                                            st = s.to_owned();
                                        } else {
                                            let new_statement = tx.read().await.as_ref().unwrap().prepare(&s.s).await?.clone();
                                            prepared.insert((&s.s).clone(), new_statement.clone());
                                            st = new_statement;
                                        }

                                        let tx1  =tx.clone();
                                        futures.push(async move {
                                            let p1 = s.params.iter().map(move |x| x.as_ref() as _).collect::<Vec<_>>();
                                            match tx1.read().await.as_ref().unwrap().execute(&st, &p1).await {
                                                Ok(_) => Ok(()),
                                                Err(e) => {
                                                    Err(MyError::from(e))
                                                }
                                            }
                                        });
                                    }
                                    Flush =>{
                                        while futures.len() > 0 {
                                            futures.next().await;
                                        }
                                    }
                                    Commit => {
                                        while futures.len() > 0 {
                                            futures.next().await;
                                        }

                                        let the_tx = tx.clone().write().await.take();
                                        match the_tx {
                                            None => {
                                                panic!("bug.")
                                            }
                                            Some(x) => {
                                                x.commit().await?;
                                            }
                                        }
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

            println!("exited loop");

            Ok(())
        });

        done_tx.send(jh).expect("BUG: failed to send done join handle");

        Execer {
            tx_commands: Some(commands_tx),
            done_rx,
        }
    }

    async fn submit(&mut self, cmd: Command) -> Result<(), MyError> {
        match &mut self.tx_commands {
            None => Err(MyError::from("Closed")),
            Some(tx_commands) => {
                tx_commands.send(cmd).await?;
                Ok(())
            }
        }
    }

    async fn close(&mut self) -> std::result::Result<(), MyError> {
        self.tx_commands = None; // cause Sender to be dropped.
        self.done_rx.try_recv()?.await?
    }
}
struct Stmt {
    s: String,
    params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>>,
    //params: Vec<&'a (dyn tokio_postgres::types::ToSql + Sync)>,
}

enum Command {
    Statement(Stmt),
    Flush,
    Commit,
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
        .submit(Statement(Stmt {
            s: "create table if not exists test01 (id bigint,created_at TIMESTAMPTZ DEFAULT (Now() at time zone 'utc'));"
                .to_string(),
            params: vec![],
        }))
        .await?;

    execer.submit(Flush).await?;

    for i in 1_i64..1000000 {
        let cmd = Statement(Stmt {
            s: "insert into test01 values($1)".to_string(),
            params: vec![Box::new(i)],
        });
        execer.submit(cmd).await?;
    }

    execer.submit(Commit).await?;

    execer.close().await?;

    println!("success");

    Ok(())
}
#[derive(Debug)]
struct MyError {
    s: String,
}
impl std::convert::From<tokio::task::JoinError> for MyError {
    fn from(e: tokio::task::JoinError) -> Self {
        MyError {
            s: String::from(e.to_string()),
        }
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
