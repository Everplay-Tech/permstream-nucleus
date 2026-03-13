use super::*;

#[test]
fn test_fenwick_tree_logic() {
    let mut model = [1i64; 256];
    // Heavily skew the model
    model[0] = 500;
    model[255] = 1000;
    
    let ft = arithmetic::FenwickTree::new(&model);
    
    // Verify query
    assert_eq!(ft.query(0), 0); // Elements BEFORE index 0
    assert_eq!(ft.query(1), 500); // Elements BEFORE index 1
    
    let mut total_sum = 0;
    for i in 0..255 {
        total_sum += model[i];
    }
    assert_eq!(ft.query(255), total_sum);
    
    let full_sum = total_sum + model[255];
    
    // Verify find_symbol
    // Target 250 -> should be symbol 0 (since it has prob 500)
    let (sym1, cum1) = ft.find_symbol(250);
    assert_eq!(sym1, 0);
    assert_eq!(cum1, 0);
    
    // Target full_sum - 1 -> should be symbol 255
    let (sym2, cum2) = ft.find_symbol(full_sum - 1);
    assert_eq!(sym2, 255);
    assert_eq!(cum2, total_sum);
}
