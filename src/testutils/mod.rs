#[cfg(test)]
pub mod fs {
    use anyhow::Result;
    use std::fs;

    pub fn compare_files(file_path1: &str, file_path2: &str) -> Result<bool> {
        // Read the contents of the first file into a vector
        let contents1 = fs::read(file_path1).expect(&format!("Cant't read file {file_path1}"));
        // Read the contents of the second file into a vector
        let contents2 = fs::read(file_path2).expect(&format!("Cant't read file {file_path2}"));
        Ok(contents1 == contents2)
    }
}
