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

#[derive(Debug, Clone)]
pub struct PageHeader {
    pub page_id: u32,
    pub tuple_count: u16,
    pub free_space: u16,
}

impl PageHeader {
    pub fn new(page_id: u32) -> Self {
        Self {
            page_id,
            tuple_count: 0,
            free_space: 4096, // 4KB default
        }
    }
}

pub struct Page {
    pub header: PageHeader,
    pub slot_array: Vec<Option<usize>>, // Maps slot_id to index in data vector
    pub data: Vec<Vec<u8>>, // Vector of tuples (each tuple is a Vec<u8>)
}

impl Page {
    pub fn new(page_id: u32) -> Self {
        Self {
            header: PageHeader::new(page_id),
            slot_array: Vec::new(),
            data: Vec::new(),
        }
    }
    
    /// Create a new tuple and return its slot ID
    pub fn create_tuple(&mut self, tuple_data: Vec<u8>) -> usize {
        let data_index = self.data.len();
        self.data.push(tuple_data);

        let slot_id = self.slot_array.len();
        self.slot_array.push(Some(data_index));

        self.header.tuple_count += 1;
        slot_id
    }

    /// Read a tuple by slot ID
    pub fn read_tuple(&self, slot_id: usize) -> Result<&Vec<u8>, PageError> {
        if slot_id >= self.slot_array.len() {
            return Err(PageError::InvalidSlot);
        }

        match self.slot_array[slot_id] {
            Some(data_index) => {
                if data_index < self.data.len() {
                    Ok(&self.data[data_index])
                } else {
                    Err(PageError::TupleNotFound)
                }
            },
            None => Err(PageError::TupleNotFound),
        }
    }

    /// Update a tuple by slot ID
    pub fn update_tuple(&mut self, slot_id: usize, new_data: Vec<u8>) -> Result<(), PageError> {
        if slot_id >= self.slot_array.len() {
            return Err(PageError::InvalidSlot);
        }

        match self.slot_array[slot_id] {
            Some(data_index) => {
                if data_index < self.data.len() {
                    self.data[data_index] = new_data;
                    Ok(())
                } else {
                    Err(PageError::TupleNotFound)
                }
            }
            None => Err(PageError::TupleNotFound),
        }
    }

    /// Delete a tuple by slot ID (marks slot as empty)
    pub fn delete_tuple(&mut self, slot_id: usize) -> Result<(), PageError> {
        if slot_id >= self.slot_array.len() {
            return Err(PageError::InvalidSlot);
        }

        if self.slot_array[slot_id].is_none() {
            return Err(PageError::TupleNotFound);
        }

        self.slot_array[slot_id] = None;
        self.header.tuple_count -= 1;
        Ok(())
    }

    /// Get the number of active tuples
    pub fn tuple_count(&self) -> u16 {
        self.header.tuple_count
    }

    /// Check if a slot is valid and contains data
    pub fn is_slot_valid(&self, slot_id: usize) -> bool {
        slot_id < self.slot_array.len() && self.slot_array[slot_id].is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_creation() {
        let page = Page::new(1);
        assert_eq!(page.header.page_id, 1);
        assert_eq!(page.header.tuple_count, 0);
        assert_eq!(page.slot_array.len(), 0);
        assert_eq!(page.data.len(), 0);
    }

    #[test]
    fn test_create_tuple() {
        let mut page = Page::new(1);
        let tuple_data = vec![1, 2, 3, 4];
        
        let slot_id = page.create_tuple(tuple_data.clone());
        
        assert_eq!(slot_id, 0);
        assert_eq!(page.tuple_count(), 1);
        assert_eq!(page.data.len(), 1);
        assert_eq!(page.slot_array.len(), 1);
        assert_eq!(page.slot_array[0], Some(0));
    }

    #[test]
    fn test_read_tuple() {
        let mut page = Page::new(1);
        let tuple_data = vec![1, 2, 3, 4];
        
        let slot_id = page.create_tuple(tuple_data.clone());
        let read_data = page.read_tuple(slot_id).unwrap();
        
        assert_eq!(read_data, &tuple_data);
    }

    #[test]
    fn test_update_tuple() {
        let mut page = Page::new(1);
        let original_data = vec![1, 2, 3, 4];
        let updated_data = vec![5, 6, 7, 8];
        
        let slot_id = page.create_tuple(original_data);
        page.update_tuple(slot_id, updated_data.clone()).unwrap();
        
        let read_data = page.read_tuple(slot_id).unwrap();
        assert_eq!(read_data, &updated_data);
    }

    #[test]
    fn test_delete_tuple() {
        let mut page = Page::new(1);
        let tuple_data = vec![1, 2, 3, 4];
        
        let slot_id = page.create_tuple(tuple_data);
        assert_eq!(page.tuple_count(), 1);
        
        page.delete_tuple(slot_id).unwrap();
        assert_eq!(page.tuple_count(), 0);
        assert!(!page.is_slot_valid(slot_id));
        
        let result = page.read_tuple(slot_id);
        assert_eq!(result, Err(PageError::TupleNotFound));
    }

    #[test]
    fn test_multiple_tuples() {
        let mut page = Page::new(1);
        
        let tuple1 = vec![1, 2];
        let tuple2 = vec![3, 4];
        let tuple3 = vec![5, 6];
        
        let slot1 = page.create_tuple(tuple1.clone());
        let slot2 = page.create_tuple(tuple2.clone());
        let slot3 = page.create_tuple(tuple3.clone());
        
        assert_eq!(slot1, 0);
        assert_eq!(slot2, 1);
        assert_eq!(slot3, 2);
        assert_eq!(page.tuple_count(), 3);
        
        assert_eq!(page.read_tuple(slot1).unwrap(), &tuple1);
        assert_eq!(page.read_tuple(slot2).unwrap(), &tuple2);
        assert_eq!(page.read_tuple(slot3).unwrap(), &tuple3);
    }

    #[test]
    fn test_invalid_slot_errors() {
        let page = Page::new(1);
        
        // Test reading from non-existent slot
        let result = page.read_tuple(0);
        assert_eq!(result, Err(PageError::InvalidSlot));
        
        // Test reading from out-of-bounds slot
        let result = page.read_tuple(100);
        assert_eq!(result, Err(PageError::InvalidSlot));
    }

    #[test]
    fn test_tuple_not_found_errors() {
        let mut page = Page::new(1);
        let tuple_data = vec![1, 2, 3];
        
        let slot_id = page.create_tuple(tuple_data);
        page.delete_tuple(slot_id).unwrap();
        
        // Try to read deleted tuple
        let result = page.read_tuple(slot_id);
        assert_eq!(result, Err(PageError::TupleNotFound));
        
        // Try to update deleted tuple
        let result = page.update_tuple(slot_id, vec![4, 5, 6]);
        assert_eq!(result, Err(PageError::TupleNotFound));
        
        // Try to delete already deleted tuple
        let result = page.delete_tuple(slot_id);
        assert_eq!(result, Err(PageError::TupleNotFound));
    }

    #[test]
    fn test_is_slot_valid() {
        let mut page = Page::new(1);
        let tuple_data = vec![1, 2, 3];
        
        // Invalid slot (doesn't exist)
        assert!(!page.is_slot_valid(0));
        
        let slot_id = page.create_tuple(tuple_data);
        
        // Valid slot
        assert!(page.is_slot_valid(slot_id));
        
        page.delete_tuple(slot_id).unwrap();
        
        // Invalid slot (deleted)
        assert!(!page.is_slot_valid(slot_id));
    }
}