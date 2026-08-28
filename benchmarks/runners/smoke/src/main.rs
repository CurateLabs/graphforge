fn main() {
    println!("graphforge external benchmark runner smoke");
}

#[cfg(test)]
mod tests {
    #[test]
    fn runner_is_namespaced_as_a_benchmark_binary() {
        assert!(env!("CARGO_PKG_NAME").starts_with("graphforge-benchmark-"));
    }
}
