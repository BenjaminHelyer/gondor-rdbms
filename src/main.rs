use gondor_rdbms::storage::Page;

fn main() {
    let s1 = String::from("Hello and welcome to the Gondor RDBMS!");
    let len = calculate_length(&s1);
    println!("The length of '{s1}' is {len}.");

    let page = Page::new(1);
}

fn calculate_length(s: &String) -> usize {
    let length = s.len();

    length
}
