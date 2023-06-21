use std::{
    ops::{Add, AddAssign, Deref},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

use async_recursion::async_recursion;
use async_walkdir::WalkDir;
use smol::{io, net, prelude::*, spawn, Unblock};

fn main() -> io::Result<()> {
    smol::block_on(async {
        let counter = Arc::new(AtomicUsize::new(0));
        let start = Instant::now();

        //let mut counter: usize = 0;
        //let mut entries = async_fs::read_dir("c:\\").await;
        recursive_dir_search("c:\\".to_owned(), counter.clone()).await?;

        println!(
            "files per sec : {}/{}={}",
            counter.load(Ordering::SeqCst),
            start.elapsed().as_secs(),
            counter.load(Ordering::SeqCst) / start.elapsed().as_secs() as usize
        );
        let start = Instant::now();
        let mut counter_2: usize = 0;
        let mut entries = WalkDir::new("c:\\");
        let mut tasks = vec![];
        loop {
            match entries.next().await {
                Some(Ok(entry)) => {
                    counter_2 += 1;
                    if entry.metadata().await?.is_dir() {
                        tasks.push(spawn(async move {
                            let mut counter = 0;
                            let mut entries = WalkDir::new(entry.path().clone());
                            loop {
                                match entries.next().await {
                                    Some(Ok(entry)) => {
                                        counter += 1;
                                    }
                                    Some(Err(e)) => {
                                        counter += 1;
                                        //                    eprintln!("error: {}", e);
                                        continue;
                                    }
                                    None => break,
                                }
                            }
                            counter
                        }));
                        continue;
                    }
                }
                Some(Err(e)) => {
                    counter_2 += 1;
                    //                    eprintln!("error: {}", e);
                    continue;
                }
                None => break,
            }
        }
        for task in tasks {
            counter_2 += task.await;
        }
        println!(
            "files per sec : {}/{}={}",
            counter_2,
            start.elapsed().as_secs(),
            counter_2 / start.elapsed().as_secs() as usize
        );

        Ok(())
    })
}

#[async_recursion]
async fn recursive_dir_search(path: String, counter: Arc<AtomicUsize>) -> io::Result<()> {
    let mut entries = match async_fs::read_dir(path).await {
        Ok(entries) => entries,
        Err(e) => {
            //            println!("Error: {} on {}", e, path);
            return Ok(());
        }
    };
    let mut handles = Vec::new();
    while let Some(entry) = entries.next().await {
        match entry {
            Ok(entry) => {
                if entry.file_type().await?.is_dir() {
                    handles.push(recursive_dir_search(
                        entry.path().to_str().unwrap().to_owned(),
                        counter.clone(),
                    ));
                } else {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
            }
            Err(e) => {
                //               println!("Error: {} on file getting entry", e);
                continue;
            }
        }
    }
    for handle in handles {
        handle.await?;
    }

    Ok(())
}
