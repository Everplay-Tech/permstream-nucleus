use std::path::PathBuf;
fn main() {
    let base = PathBuf::from("/tmp/out");
    println!("{:?}", base.join("../../etc/passwd"));
    println!("{:?}", base.join("/etc/passwd"));
}
