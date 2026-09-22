//! Composed public-facade evidence for the two-story analyst journey.
#[path = "research_journey/fixture.rs"]
mod fixture;
#[path = "research_journey/review.rs"]
mod review;
#[path = "research_journey/sharing.rs"]
mod sharing;
use fixture::*;
use uuid::Uuid;

#[test]
fn two_story_journey_preserves_scope_evidence_review_and_continued_research() {
    let mut journey = Journey::new();
    journey.explore_and_focus();
    journey.analyze_and_update();
    let accepted = journey.review_and_continue();
    journey.restore_and_retry(&accepted);
    journey.share_and_reopen(&accepted);
    journey.output.finish(&journey);
}

impl Journey {
    fn analyze_and_update(&mut self) {
        let local_edit = self.text("local_edit").to_owned();
        self.edit(self.a, &local_edit);
        self.graph.execute(self.text("upstream_edit")).unwrap();
        let before = self.generation();
        let preview_request =
            request(serde_json::json!({"branch_uuid":self.a,"scope":{"kind":"branch"}}));
        let preview = self
            .graph
            .preview_research_upstream(&preview_request, &cancel())
            .unwrap();
        assert_eq!(self.generation(), before);
        self.output.arrow("upstream-preview", &preview);
        let batch = &preview.batches[0];
        let row = (0..batch.num_rows())
            .find(|i| string(batch, "field", *i) == "property:x")
            .unwrap();
        let digest = hex_bytes(&preview.schema.metadata()["graphforge.upstream.preview_sha256"]);
        let update = request(serde_json::json!({
            "operation_uuid":Uuid::now_v7(),"expected_generation_uuid":before,"version_uuid":Uuid::now_v7(),
            "preview":preview_request,"preview_sha256":digest,
            "selection":{"kind":"selected","decisions":[{"unit":{"object_kind":"node","object_uuid":binary_uuid(batch,"object_uuid",row),"field":"property:x"},"resolution":{"kind":"adopt_upstream"}}]},
            "acknowledge_evidence":[],"actor_uuid":self.actor,"created_at":3,"explanation":"Reviewed x only"
        }));
        let receipt = self
            .graph
            .update_research_branch(&update, &cancel())
            .unwrap();
        self.output.json("upstream-receipt", &receipt);
        assert_eq!(
            self.graph
                .update_research_branch(&update, &cancel())
                .unwrap(),
            receipt
        );
        let view = self.graph.open_research_branch(self.a).unwrap();
        let analysis = view
            .graph()
            .execute("MATCH(n:Character) RETURN n.score AS score,n.x AS x,n.y AS y")
            .unwrap();
        assert_eq!(
            (
                integer(&analysis, "score"),
                integer(&analysis, "x"),
                integer(&analysis, "y")
            ),
            (1, 1, 0)
        );
        assert_eq!(
            integer(
                &self
                    .graph
                    .execute("MATCH(n:Character) RETURN n.score AS score")
                    .unwrap(),
                "score"
            ),
            0
        );
        self.output.arrow("branch-analysis", &analysis);
        assert_eq!(
            self.graph
                .open_research_version(view.version_uuid())
                .unwrap()
                .artifact_payload(self.ocr)
                .unwrap()
                .batches[0]
                .num_rows(),
            1
        );
        drop(view);
        let comparison = request(
            serde_json::json!({"left":{"kind":"branch","branch_uuid":self.a},"right":{"kind":"project"},"left_authority":null,"right_authority":null,"detail":"changes","accepted":[],"max_fields":40000,"max_bytes":67108864,"page_size":100,"after":null}),
        );
        let result = self.graph.compare_research(&comparison, &cancel()).unwrap();
        let fields: Vec<_> = result
            .batches
            .iter()
            .flat_map(|b| (0..b.num_rows()).map(|i| string(b, "field", i)))
            .collect();
        assert!(fields.contains(&"property:score".to_owned()));
        assert!(fields.contains(&"property:ending".to_owned()));
        self.output.arrow("comparison", &result);
    }
}
