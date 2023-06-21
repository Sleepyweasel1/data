use async_fs::windows::MetadataExt;
use async_recursion::async_recursion;
use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use std::{io::Write, time::Instant, borrow::Cow};
use tokio::sync::mpsc::{self, Sender};
use walkdir::WalkDir;
use surrealdb::{Surreal, engine::remote::ws::Ws, opt::auth::Root};

#[derive(Debug, Serialize, Deserialize)]
enum FileType {
    File,
    Directory,
}

#[derive(Debug, Serialize, Deserialize)]
struct FileData {
    path: Cow<'static, str>,
    size: u64,
    #[serde(rename = "type")]
    f_type: FileType,
    created: DateTime<Utc>,
    modified: DateTime<Utc>,
    accessed: DateTime<Utc>,
}
#[tokio::main]
async fn main() {
    let db = Surreal::new::<Ws>("127.0.0.1:8000").await.expect("Unable to connect to server");
    db.signin(Root {
        username: "root",
        password: "root",
    })
    .await.expect("Unable to sign in");
    db.use_ns("files").use_db("files").await.expect("Unable to connect to namespace and db");
    let (tx, mut rx) = mpsc::channel::<FileData>(10000);
    let start = Instant::now();
    tokio::spawn(async move {
    let (_, _) = parse_dir_ar("C:\\", tx.clone()).await;
    });
    let mut file_counter = 0;
    let mut db_handles = vec![];
    while let Some(file) = rx.recv().await {
        file_counter += 1;
        let db = db.clone();
        db_handles.push(tokio::spawn(async move {
            let _ = db.create(("files", file.path.to_string())).content(file).await.unwrap_or(FileData {
                path: Cow::from(""),
                size: 0,
                f_type: FileType::File,
                created: Utc::now(),
                modified: Utc::now(),
                accessed: Utc::now(),
            });
        }));
    }
    for handle in db_handles {
        handle.await.unwrap();
    }
    let duration = start.elapsed();

    println!(
        "Total files: {} in {} seconds, which is  files per second",
        file_counter,
        duration.as_secs(),
    );
    /*
    println!(
        "Total files: {} in {} seconds, which is {} files per second",
        counter,
        duration.as_secs(),
        counter / duration.as_secs() as usize
    );

    //write scanned_files to file
    let mut file =
        std::fs::File::create("c:\\temp\\scanned_files.txt").expect("Unable to create file");
    for lines in scanned_files {
        write!(file, "{}\n", lines).expect("Unable to write data");
    }
    */
}

fn parse_dir(dir: &str) -> (usize, Vec<String>) {
    let mut counter: usize = 0;
    let mut files: Vec<String> = vec![];
    for entry in WalkDir::new(dir).max_open(64).into_iter().filter_map(|e| e.ok()) {
        /*
        let file = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        */
        //files.push(file.path().display().to_string());
        files.push(format!(
            "{},{}",
            entry.path().display().to_string(),
            entry.metadata().expect("No metadata").file_size()
        ));
        counter += 1;
    }
    (counter, files)
}

#[async_recursion]
async fn parse_dir_ar(dir: &str, sender: Sender<FileData>) -> (usize, Vec<String>) {
    let mut counter: usize = 0;
    let mut handles = vec![];
    let mut scanned_files = vec![];
    println!("parse_dir_ar: {}", dir);
    for entry in WalkDir::new(dir).min_depth(1).max_depth(1).into_iter().filter_map(|e| e.ok()) {

        if entry.metadata().unwrap().is_dir() {
            
            counter += 1;
           // println!("{:?}", DateTime::<Utc>::from(entry.metadata().expect("No metadata").created().expect("No created")).to_rfc2822());
            sender.send(FileData {
                path: entry.path().display().to_string().into(),
                size: entry.metadata().expect("No metadata").file_size(),
                f_type: FileType::Directory,
                created: DateTime::<Utc>::from(entry.metadata().expect("No metadata").created().expect("No created")),
                accessed: DateTime::<Utc>::from(entry.metadata().expect("No metadata").accessed().expect("No accessed")),
                modified: DateTime::<Utc>::from(entry.metadata().expect("No metadata").modified().expect("No modified")),
            }).await.expect("Failed to send");
           
           let sender = sender.clone();
            handles.push(tokio::spawn(async move {
                let files = parse_dir_ar(entry.path().display().to_string().as_str(), sender).await;
                files
            }));
        } else {
            sender.send(FileData {
                path: entry.path().display().to_string().into(),
                size: entry.metadata().expect("No metadata").file_size(),
                f_type: FileType::File,
                created: DateTime::<Utc>::from(entry.metadata().expect("No metadata").created().expect("No created")),
                accessed: DateTime::<Utc>::from(entry.metadata().expect("No metadata").accessed().expect("No accessed")),
                modified: DateTime::<Utc>::from(entry.metadata().expect("No metadata").modified().expect("No modified")),
            }).await.expect("Failed to send");
            scanned_files.push(format!(
                "{},{}",
                entry.path().display().to_string(),
                entry.metadata().expect("No metadata").file_size()
            ));
            //println!("{:?}", DateTime::<Utc>::from(entry.metadata().expect("No metadata").created().expect("No created")).to_rfc2822());
            counter += 1;
        }
    }
    for handle in handles {
        let (count, files) = handle.await.unwrap();

        counter += count;
        scanned_files.extend(files);
    }
    (counter, scanned_files)
}
