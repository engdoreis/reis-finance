#[cfg(test)]
pub mod test {
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
}
pub fn epoc_to_date(column: &str) -> Expr {
    (col(column) * lit(1000))
        .cast(DataType::Datetime(datatypes::TimeUnit::Milliseconds, None))
        .cast(DataType::Date)
}

pub fn str_to_date(column: &str) -> Expr {
    col(column)
        .str()
        .replace(
            lit(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*"),
            lit(r"$1"),
            false,
        )
        .str()
        .to_datetime(None, None, StrptimeOptions::default(), lit("raise"))
        .cast(DataType::Date)
}
