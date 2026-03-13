use super::*;

#[test]
fn test_bitplane_transform() {
    // Mock 4-byte floating point structures
    let mut data = vec![0u8; 12];
    data[0] = 0xAA; data[1] = 0x11; data[2] = 0x22; data[3] = 0x33;
    data[4] = 0xBB; data[5] = 0x44; data[6] = 0x55; data[7] = 0x66;
    data[8] = 0xCC; data[9] = 0x77; data[10] = 0x88; data[11] = 0x99;
    
    let transformed = transforms::apply_bitplane(&data);
    
    // With 4-byte stride, first 3 bytes should be the first bytes of each float (AA, BB, CC)
    assert_eq!(transformed[0], 0xAA);
    assert_eq!(transformed[1], 0xBB);
    assert_eq!(transformed[2], 0xCC);
    
    // Next 3 should be the second bytes (11, 44, 77)
    assert_eq!(transformed[3], 0x11);
    assert_eq!(transformed[4], 0x44);
    assert_eq!(transformed[5], 0x77);
    
    let inverted = transforms::invert_bitplane(&transformed);
    
    assert_eq!(data, inverted);
}
