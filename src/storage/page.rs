#[derive(Debug, PartialEq)]
pub enum PageError {
    TupleNotFound,
    InvalidSlot,
    NotEnoughSpace,
}

impl std::fmt::Display for PageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PageError::TupleNotFound => write!(f, "Tuple not found"),
            PageError::InvalidSlot => write!(f, "Invalid slot"),
            PageError::NotEnoughSpace => write!(f, "Not enough space"),
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

    pub fn insert_tuple(&mut self, tuple: &[u8]) -> Result<u16, PageError> {
        let header = self.get_header();

        if header.free_space_total < tuple.len() as u16 {
            return Err(PageError::NotEnoughSpace);
        }

        // data and slot array grow towards each other, so slot array ends where free space begins
        let slot_offset = header.offset_begin_free_space;
        let slot_id = (slot_offset - HEADER_SIZE as u16) / 2;

        // data and slot array grow towards each other, so data array begins where free space ends
        let tuple_offset_end = header.offset_end_free_space;
        let tuple_offset_begin = tuple_offset_end - tuple.len() as u16; // end of tuple should be where free space ends
        
        // modify slot array to point to the new tuple
        self.update_slot(slot_id, tuple_offset_begin, tuple.len() as u16)?;

        // copy tuple into the appropraite slot on page
        let tuple_data = &mut self.contents[tuple_offset_begin as usize..tuple_offset_end as usize];
        tuple_data.copy_from_slice(tuple);

        // calculate new free space
        let new_free_space_total = header.free_space_total - tuple.len() as u16;
        let new_offset_begin_free_space = slot_offset + 4;
        let new_offset_end_free_space = tuple_offset_begin - 1;

        self.update_header(new_free_space_total, new_offset_begin_free_space, new_offset_end_free_space);

        // return slot id of the new tuple
        Ok(slot_id)
    }

    fn update_slot(&mut self, slot_id: u16, tuple_offset_begin: u16, tuple_length: u16) -> Result<(), PageError> {
        let slot_offset = HEADER_SIZE as u16 + slot_id * 2;

        if slot_offset + 4 > PAGE_SIZE as u16 {
            return Err(PageError::InvalidSlot);
        } else if slot_offset + 4 > self.get_header().offset_end_free_space {
            // this case would mean we are trying to modify slot array data that is in the data space
            // typically this means we do not have enough space
            return Err(PageError::NotEnoughSpace);
        }

        // update slot array data
        let slot_data = &mut self.contents[slot_offset as usize..(slot_offset + 4) as usize];
        slot_data[0] = (tuple_offset_begin & 0xFF) as u8; // get lower 8 bits -- mask upper 8 bits of offset
        slot_data[1] = ((tuple_offset_begin >> 8) & 0xFF) as u8; // get upper 8 bits -- shift and mask upper 8 bits of offset (should be 0, but just in case)
        slot_data[2] = (tuple_length & 0xFF) as u8; // get lower 8 bits -- mask upper 8 bits of length
        slot_data[3] = ((tuple_length >> 8) & 0xFF) as u8; // get upper 8 bits -- shift and mask upper 8 bits of length (should be 0, but just in case)

        // update header with new free space beginning, since slot array is now extended
        let header = self.get_header();
        let new_offset_begin_free_space = slot_offset + 4;
        self.update_header(header.free_space_total, new_offset_begin_free_space, header.offset_end_free_space);

        Ok(())
    }

    fn update_header(&mut self, new_free_space_total: u16, new_offset_begin_free_space: u16, new_offset_end_free_space: u16) -> Result<(), PageError> {
        let header_bytes = &mut self.contents[0..HEADER_SIZE];
        header_bytes[4] = (new_free_space_total & 0xFF) as u8; // get lower 8 bits
        header_bytes[5] = ((new_free_space_total >> 8) & 0xFF) as u8; // get upper 8 bits
        header_bytes[6] = (new_offset_begin_free_space & 0xFF) as u8; // get lower 8 bits
        header_bytes[7] = ((new_offset_begin_free_space >> 8) & 0xFF) as u8; // get upper 8 bits
        header_bytes[8] = (new_offset_end_free_space & 0xFF) as u8; // get lower 8 bits
        header_bytes[9] = ((new_offset_end_free_space >> 8) & 0xFF) as u8; // get upper 8 bits

        Ok(())
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

    #[test]
    fn test_insert_tuple() {
        let mut page = Page::new(1);
        let tuple = b"Hello, world!";
        let slot_id = page.insert_tuple(tuple).unwrap();
        assert_eq!(slot_id, 0);
        let retrieved_tuple = page.get_data(slot_id).unwrap();
        assert_eq!(retrieved_tuple, tuple);
    }

    #[test]
    fn test_insert_tuple_not_enough_space() {
        let mut page = Page::new(1);
        // make page header show that there is no space left
        let header = page.get_header();
        let header_bytes = &mut page.contents[0..HEADER_SIZE];

        header_bytes[4] = 0; // no free space total
        header_bytes[5] = 0;
        header_bytes[6] = 0; // free space begin offset is index 0
        header_bytes[7] = 0;
        header_bytes[8] = 0; // free space end offset is index 0
        header_bytes[9] = 0;

        let tuple = b"Hello, world! This tuple is too long to fit in the page.";
        let result = page.insert_tuple(tuple);
        assert_eq!(result.unwrap_err(), PageError::NotEnoughSpace);
    }

    #[test]
    fn test_insert_multiple_tuples() {
        let mut page = Page::new(1);
        let tuple1 = b"Hello, world!";
        let tuple2 = b"This is a test tuple.";
        let tuple3 = b"Another test tuple.";

        let slot_id1 = page.insert_tuple(tuple1).unwrap();
        let slot_id2 = page.insert_tuple(tuple2).unwrap();
        let slot_id3 = page.insert_tuple(tuple3).unwrap();

        let data1 = page.get_data(slot_id1).unwrap();
        let data2 = page.get_data(slot_id2).unwrap();
        let data3 = page.get_data(slot_id3).unwrap();

        assert_eq!(data1, tuple1);
        assert_eq!(data2, tuple2);
        assert_eq!(data3, tuple3);
    }
}