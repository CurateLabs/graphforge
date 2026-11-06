//! Origin metadata is advisory and never reads the per-origin secret records.
use super::*;
use std::collections::BTreeMap;

#[derive(Default)]
struct Origins {
    values: RefCell<BTreeMap<String, String>>,
    reads: RefCell<Vec<String>>,
}
impl Store for Origins {
    fn lock(&self, _: &str) -> Result<Box<dyn StoreLock + '_>, GfError> {
        Ok(Box::new(()))
    }
    fn read(&self, key: &str) -> Result<Option<String>, SavedCredentialError> {
        self.reads.borrow_mut().push(key.into());
        Ok(self.values.borrow().get(key).cloned())
    }
    fn write(&self, key: &str, value: &str) -> Result<(), GfError> {
        self.values.borrow_mut().insert(key.into(), value.into());
        Ok(())
    }
    fn delete(&self, key: &str) -> Result<(), GfError> {
        self.values.borrow_mut().remove(key);
        Ok(())
    }
}

#[test]
fn origin_peek_reads_no_session_and_logout_only_clears_matching_pointer() {
    let store = Origins::default();
    let first = origin("https://first.example").unwrap();
    let second = origin("https://second.example").unwrap();
    assert!(last_login_origin_from(&store).unwrap().is_none());
    for hub in [&first, &second] {
        save(
            &store,
            hub,
            Mock::new(vec![(200, tokens("access-secret", "refresh-secret"))])
                .replies
                .borrow_mut()
                .pop_front()
                .unwrap(),
            100,
        )
        .unwrap();
    }
    store.reads.borrow_mut().clear();
    assert_eq!(
        last_login_origin_from(&store).unwrap(),
        Some(second.clone())
    );
    assert_eq!(*store.reads.borrow(), [LAST_LOGIN_ORIGIN]);
    let mock = Mock::new(vec![(204, serde_json::json!({}))]);
    logout(&store, &mock, &first, 100).unwrap();
    assert_eq!(
        last_login_origin_from(&store).unwrap(),
        Some(second.clone())
    );
    assert!(
        store
            .values
            .borrow()
            .contains_key(&second.origin().ascii_serialization())
    );
    logout(&store, &mock, &first, 100).unwrap();
    let mock = Mock::new(vec![(204, serde_json::json!({}))]);
    logout(&store, &mock, &second, 100).unwrap();
    assert!(last_login_origin_from(&store).unwrap().is_none());
    assert!(store.values.borrow().is_empty());
}

#[test]
fn multiple_hub_sessions_exchange_only_the_targets_secret() {
    let store = Origins::default();
    let first = origin("https://first.example").unwrap();
    let second = origin("https://second.example").unwrap();
    for (hub, secret) in [(&first, "first-secret"), (&second, "second-secret")] {
        save(
            &store,
            hub,
            Mock::new(vec![(200, tokens(secret, "refresh-secret"))])
                .replies
                .borrow_mut()
                .pop_front()
                .unwrap(),
            100,
        )
        .unwrap();
    }
    for (hub, secret) in [(&first, "first-secret"), (&second, "second-secret")] {
        let mock = Mock::new(vec![(
            200,
            serde_json::json!({"access_token":"scoped-token","token_type":"Bearer","expires_in":900,"origin":hub.origin().ascii_serialization(),"repository_id":"repo"}),
        )]);
        repository_token_from(&store, &mock, hub, "curate", "claims", "publish", 100)
            .unwrap()
            .unwrap()
            .into_publish_token(hub)
            .unwrap();
        assert_eq!(
            last_login_origin_from(&store).unwrap(),
            Some(second.clone())
        );
        assert_eq!(mock.requests.borrow().len(), 1);
        let request = &mock.requests.borrow()[0];
        assert_eq!(Url::parse(&request.url).unwrap().origin(), hub.origin());
        assert_eq!(
            request.header("authorization"),
            Some(format!("Bearer {secret}").as_str())
        );
    }
}

#[test]
fn unsafe_saved_metadata_is_not_printable_as_a_hub_origin() {
    let store = Origins::default();
    for value in [
        "https://user:secret@hub.example",
        "https://hub.example/path",
        "https://hub.example/?secret=value",
        "https://hub.example/#fragment",
        "http://hub.example",
    ] {
        store.write(LAST_LOGIN_ORIGIN, value).unwrap();
        assert!(last_login_origin_from(&store).is_err());
    }
    store.write(LAST_LOGIN_ORIGIN, &"x".repeat(2049)).unwrap();
    assert!(last_login_origin_from(&store).is_err());
}
