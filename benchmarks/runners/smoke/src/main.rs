fn main() {
    println!("graphforge external benchmark runner smoke");
}

#[cfg(test)]
mod tests {
    #[test]
    fn runner_has_no_product_or_third_party_dependencies() {
        assert!(env!("CARGO_PKG_NAME").starts_with("graphforge-benchmark-"));
    }
}
