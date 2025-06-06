#[derive(Debug, PartialEq)]
pub enum PageError {
    TupleNotFound,
    InvalidSlot,
}

impl std::fmt::Display for PageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PageError::TupleNotFound => write!(f, "Tuple not found"),
            PageError::InvalidSlot => write!(f, "Invalid slot"),
        }
    }
}

impl std::error::Error for PageError {}

/// size of the page header in bytes
const HEADER_SIZE: usize = 16;

/// size of a page in bytes
const PAGE_SIZE: usize = 4096;

/// Extracts and parses the page header from the raw page contents.
///
/// The header is stored in the first 16 bytes of the page and contains:
/// - Page ID (4 bytes)
/// - Free space (2 bytes)
/// - Free begin offset (2 bytes)
/// - Free end offset (2 bytes)
/// - Reserved space (6 bytes)
///
/// # Returns
///
/// A `PageHeader` struct containing the parsed header information.
///
/// # Examples
///
/// ```
/// use gondor_rdbms::storage::Page;
/// 
/// let page = Page::new(42);
/// let header = page.get_header();
/// assert_eq!(header.page_id, 42);
/// ```
#[derive(Debug, Clone)]
pub struct PageHeader {
    pub page_id: u32,
    pub free_space_total: u16,
    pub offset_begin_free_space: u16,
    pub offset_end_free_space: u16,
}

impl PageHeader {
    pub fn new(page_id: u32) -> Self {
        Self {
            page_id,
            free_space_total: (PAGE_SIZE - HEADER_SIZE) as u16,
            offset_begin_free_space: HEADER_SIZE as u16,
            offset_end_free_space: PAGE_SIZE as u16,
        }
    }
}

/// Represents a page in the database storage system.
///
/// A page is the fundamental unit of storage in the database, containing both
/// a header section and the actual data. Each page has a fixed size of 4096 bytes.
///
/// The page layout is as follows:
/// - Header (16 bytes)
///   - Page ID (4 bytes)
///   - Free space (2 bytes)
///   - Free space begin offset (2 bytes)
///   - Free space end offset (2 bytes)
///   - Reserved space (6 bytes)
/// - Data section (4080 bytes)
///
/// # Examples
///
/// ```
/// use gondor_rdbms::storage::Page;
///
/// let page = Page::new(42);
/// assert_eq!(page.get_header().page_id, 42);
/// ```
pub struct Page {
    /// Raw contents of the page as a fixed-size byte array
    contents: [u8; PAGE_SIZE],
}

impl Page {
    pub fn new(page_id: u32) -> Self {
        let mut contents = [0u8; PAGE_SIZE];
        let header = PageHeader::new(page_id);
        
        contents[0..4].copy_from_slice(&header.page_id.to_le_bytes());
        contents[4..6].copy_from_slice(&header.free_space_total.to_le_bytes());
        contents[6..8].copy_from_slice(&header.offset_begin_free_space.to_le_bytes());
        contents[8..10].copy_from_slice(&header.offset_end_free_space.to_le_bytes());
        
        Self { contents }
    }

    pub fn get_header(&self) -> PageHeader {
        let header_bytes = &self.contents[0..HEADER_SIZE];
        let page_id = u32::from_le_bytes([header_bytes[0], header_bytes[1], header_bytes[2], header_bytes[3]]);
        let free_space_total = u16::from_le_bytes([header_bytes[4], header_bytes[5]]);
        let offset_begin_free_space = u16::from_le_bytes([header_bytes[6], header_bytes[7]]);
        let offset_end_free_space = u16::from_le_bytes([header_bytes[8], header_bytes[9]]);

        PageHeader {
            page_id,
            free_space_total,
            offset_begin_free_space,
            offset_end_free_space,
        }

    }

    pub fn get_data(&self, slot_id: u16) -> Result<&[u8], PageError> {
        let header = self.get_header();
        let slot_offset = HEADER_SIZE as u16 + slot_id * 2;

        if slot_offset >= header.offset_begin_free_space {
            return Err(PageError::InvalidSlot);
        }

        let slot_data = &self.contents[slot_offset as usize..(slot_offset + 4) as usize];
        let tuple_offset = u16::from_le_bytes([slot_data[0], slot_data[1]]);
        let tuple_length = u16::from_le_bytes([slot_data[2], slot_data[3]]);

        if tuple_offset + tuple_length > PAGE_SIZE as u16 {
            return Err(PageError::TupleNotFound);
        }
        else if tuple_offset + tuple_length < header.offset_begin_free_space {
            return Err(PageError::TupleNotFound);
        }

        Ok(&self.contents[tuple_offset as usize..(tuple_offset + tuple_length) as usize])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_creation() {
        let page = Page::new(1);
        assert_eq!(page.get_header().page_id, 1);
        assert_eq!(page.get_header().free_space_total, (PAGE_SIZE - HEADER_SIZE) as u16);
        assert_eq!(page.get_header().offset_begin_free_space, HEADER_SIZE as u16);
        assert_eq!(page.get_header().offset_end_free_space, PAGE_SIZE as u16);
    }
}