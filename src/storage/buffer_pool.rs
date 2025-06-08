use super::Page;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;

#[derive(Debug)]
pub enum BufferPoolError {
    PageNotFound,
    IoError(std::io::Error),
}

impl From<std::io::Error> for BufferPoolError {
    fn from(error: std::io::Error) -> Self {
        BufferPoolError::IoError(error)
    }
}

pub struct BufferPool {
    page_paths: HashMap<u32, String>,
    pages: HashMap<u32, Page>,
}

impl BufferPool {
    pub fn new() -> Self {
        Self { 
            page_paths: HashMap::new(),
            pages: HashMap::new() 
        }
    }

    pub fn read_page_from_disk(&mut self, page_id: u32) -> Result<&Page, BufferPoolError> {
        let page_path = self.page_paths.get(&page_id).ok_or(BufferPoolError::PageNotFound)?;
        let mut file = File::open(page_path)?;
        let mut contents = Vec::new();
        file.read_to_end(&mut contents)?;
        let mut page = Page::new(page_id);
        page.set_contents(&contents).map_err(|_| BufferPoolError::IoError(
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid page contents")
        ))?;
        self.pages.insert(page_id, page);
        Ok(self.pages.get(&page_id).unwrap())
    }

    pub fn add_page_path(&mut self, page_id: u32, path: String) {
        self.page_paths.insert(page_id, path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;

    #[test]
    fn test_read_page_from_disk() {
        // Create a valid page in memory with page_id = 42
        let page_id = 42u32;
        let page = Page::new(page_id);
        
        // Get the raw page contents (should be 4096 bytes with valid header)
        let page_contents = page.get_raw_contents();
        
        // Create a temporary file and write the page contents to it
        let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");
        temp_file.write_all(page_contents).expect("Failed to write to temp file");
        let temp_path = temp_file.path().to_string_lossy().to_string();
        
        // Create a buffer pool and add the page path
        let mut buffer_pool = BufferPool::new();
        buffer_pool.add_page_path(page_id, temp_path);
        
        // Read the page from disk
        let read_page = buffer_pool.read_page_from_disk(page_id).expect("Failed to read page from disk");
        
        // Verify the page was read correctly
        let header = read_page.get_header();
        assert_eq!(header.page_id, page_id);
        assert_eq!(header.free_space_total, 4080); // PAGE_SIZE - HEADER_SIZE = 4096 - 16
        assert_eq!(header.offset_begin_free_space, 16); // HEADER_SIZE
        assert_eq!(header.offset_end_free_space, 4096); // PAGE_SIZE
    }
}