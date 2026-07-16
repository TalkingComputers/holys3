use anyhow::Result;
use std::path::PathBuf;

/// Platform cache root: `$XDG_CACHE_HOME`, else `~/.cache`.
pub fn cache_home() -> Result<PathBuf> {
    read_cache_home(
        std::env::var("XDG_CACHE_HOME"),
        // HOME on unix; USERPROFILE is the Windows equivalent
        std::env::var("HOME").or_else(|_| std::env::var("USERPROFILE")),
    )
}

pub fn read_cache_home(
    xdg_cache_home: std::result::Result<String, std::env::VarError>,
    home: std::result::Result<String, std::env::VarError>,
) -> Result<PathBuf> {
    match xdg_cache_home {
        // The XDG spec: an empty value means unset, and relative paths are
        // invalid and must be ignored.
        Ok(path) if !path.is_empty() && std::path::Path::new(&path).is_absolute() => {
            Ok(PathBuf::from(path))
        }
        Ok(_) | Err(std::env::VarError::NotPresent) => Ok(PathBuf::from(home.map_err(|_| {
            anyhow::anyhow!("neither XDG_CACHE_HOME, HOME, nor USERPROFILE is set")
        })?)
        .join(".cache")),
        Err(err) => Err(err.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env::VarError;

    #[test]
    fn read_cache_home_uses_xdg_cache_home() {
        let path = read_cache_home(Err(VarError::NotPresent), Ok("/home/me".to_owned())).unwrap();
        assert_eq!(path, PathBuf::from("/home/me/.cache"));

        #[cfg(windows)]
        let absolute = "C:\\cache";
        #[cfg(not(windows))]
        let absolute = "/cache";
        let path = read_cache_home(Ok(absolute.to_owned()), Err(VarError::NotPresent)).unwrap();
        assert_eq!(path, PathBuf::from(absolute));
    }

    #[test]
    fn empty_or_relative_xdg_cache_home_falls_back_to_home() {
        let path = read_cache_home(Ok(String::new()), Ok("/home/me".to_owned())).unwrap();
        assert_eq!(path, PathBuf::from("/home/me/.cache"));

        let path =
            read_cache_home(Ok("relative/cache".to_owned()), Ok("/home/me".to_owned())).unwrap();
        assert_eq!(path, PathBuf::from("/home/me/.cache"));
    }
}
