use std::path::PathBuf;

const CONFIG_DIR: &str = ".config/reis-finance";

pub fn get_config_dir() -> PathBuf {
    dirs::home_dir().unwrap().join(CONFIG_DIR)
}

pub fn get_cache_dir() -> PathBuf {
    get_config_dir().join("cache")
}
