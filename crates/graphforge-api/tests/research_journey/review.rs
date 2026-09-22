use super::fixture::*;
use graphforge_api::*;
use serde_json::json;
use uuid::Uuid;

pub struct Accepted {
    pub version: Uuid,
    pub review: ReviewResearchProposalRequest,
    pub receipt: ResearchOperationReceipt,
}
impl Journey {
    fn submit(&mut self, version: Uuid, fields: &[&str]) -> Uuid {
        let proposal = Uuid::now_v7();
        self.graph.submit_research_proposal(&request(json!({"operation_uuid":Uuid::now_v7(),"expected_generation_uuid":self.generation(),"proposal_uuid":proposal,"source_branch_uuid":self.a,"source_version_uuid":version,"frozen_ipc":self.node_capsule(version),"fields":fields.iter().map(|f|json!({"object_kind":"node","object_uuid":self.ada,"field":f})).collect::<Vec<_>>(),"actor_uuid":self.actor,"created_at":5,"motivation":"Review selected work","policy":""})),&cancel()).unwrap();
        proposal
    }
    fn review_request(&self, proposal: Uuid, partial: bool) -> ReviewResearchProposalRequest {
        let generation = self.generation();
        let preview = self
            .graph
            .preview_research_proposal(
                &PreviewResearchProposalRequest {
                    proposal_uuid: proposal,
                },
                &cancel(),
            )
            .unwrap();
        assert_eq!(generation, self.generation());
        self.output.arrow(
            if partial {
                "proposal-preview"
            } else {
                "reproposal-preview"
            },
            &preview,
        );
        let batch = &preview.batches[0];
        let fields: std::collections::BTreeSet<_> = (0..batch.num_rows())
            .map(|i| string(batch, "field", i))
            .collect();
        let expected: std::collections::BTreeSet<_> = if partial {
            ["property:score", "property:ending"]
                .into_iter()
                .map(str::to_owned)
                .collect()
        } else {
            ["property:score"].into_iter().map(str::to_owned).collect()
        };
        assert_eq!(fields, expected);
        assert_eq!(batch.num_rows(), expected.len());
        let bytes = ipc(&preview);
        for private in [b"PRIVATE_LOCAL".as_slice(), b"private_note".as_slice()] {
            assert!(!bytes.windows(private.len()).any(|window| window == private));
        }

        let decisions: serde_json::Map<String, serde_json::Value> = (0..batch.num_rows())
            .map(|i| {
                let field = string(batch, "field", i);
                assert!(["property:score", "property:ending"].contains(&field.as_str()));
                (
                    string(batch, "item_uuid", i),
                    json!(if partial && field == "property:ending" {
                        "defer"
                    } else {
                        "accept"
                    }),
                )
            })
            .collect();
        request(
            json!({"operation_uuid":Uuid::now_v7(),"expected_generation_uuid":generation,"proposal_uuid":proposal,"preview_sha256":hex_bytes(&string(batch,"preview_sha256",0)),"decisions":decisions,"resolve_conflicts":[],"acknowledge_evidence":[],"promotions":[],"community_uuid":null,"actor_uuid":self.actor,"created_at":6,"explanation":"Explicit partial review, separate from canonical promotion","policy":""}),
        )
    }
    pub fn review_and_continue(&mut self) -> Accepted {
        let version = self.head(self.a);
        let original = self
            .graph
            .research_reference(
                &ResearchReferenceTarget::Version {
                    version_uuid: version,
                },
                &cancel(),
            )
            .unwrap();
        self.output.json("immutable-reference", &original);
        let proposal = self.submit(version, &["property:score", "property:ending"]);
        let review = self.review_request(proposal, true);
        let receipt = self
            .graph
            .review_research_proposal(&review, &cancel())
            .unwrap();
        assert_eq!(
            self.graph
                .review_research_proposal(&review, &cancel())
                .unwrap(),
            receipt
        );
        let parent=self.graph.execute("MATCH(n:Character) RETURN n.score AS score,n.ending AS ending,n.private_note AS private_note").unwrap();
        assert_eq!(integer(&parent, "score"), 1);
        assert_eq!(string(&parent.batches[0], "ending", 0), "undecided");
        assert_eq!(
            string(&parent.batches[0], "private_note", 0),
            "PRIVATE_JOURNEY"
        );
        self.output.json("acceptance-receipt", &receipt);
        let choices = self
            .graph
            .research_canonical_choices(&ResearchContext::Project, None)
            .unwrap();
        assert_eq!(
            choices.batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            0
        );
        self.output
            .arrow("canonical-choices-after-acceptance", &choices);
        self.output.arrow(
            "decision-history",
            &self
                .graph
                .research_decision_history(&ResearchContext::Project, None)
                .unwrap(),
        );
        let history = self
            .graph
            .research_proposal_history(
                &request(json!({"proposal_uuid":proposal,"detail":"accepted","page_size":100})),
                &cancel(),
            )
            .unwrap();
        assert_eq!(
            history.batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            1
        );
        self.output.arrow("accepted-history", &history);
        let continued = self.edit(self.a, "MATCH(n:Character) SET n.score=2");
        let live = self
            .graph
            .research_reference(
                &ResearchReferenceTarget::Branch {
                    branch_uuid: self.a,
                },
                &cancel(),
            )
            .unwrap();
        assert_eq!(live.version.version_uuid, continued);
        assert_ne!(live.version.version_uuid, version);
        self.output.json("continued-live-reference", &live);
        let immutable = self
            .graph
            .research_reference(
                &ResearchReferenceTarget::Version {
                    version_uuid: version,
                },
                &cancel(),
            )
            .unwrap();
        assert_eq!(immutable.version, original.version);
        assert_eq!(immutable.identity_sha256, original.identity_sha256);
        assert_eq!(immutable.genealogy, original.genealogy);
        // Resolution reports current authority; frozen content identity remains stable.
        assert_ne!(
            immutable.resolved_generation_uuid,
            original.resolved_generation_uuid
        );
        Accepted {
            version,
            review,
            receipt,
        }
    }
    pub fn restore_and_retry(&mut self, accepted: &Accepted) {
        self.edit(self.b, "MATCH(n:Character) SET n.score=73");
        self.graph
            .execute("MATCH(n:Character) SET n.score=99")
            .unwrap();
        let restored = Uuid::now_v7();
        self.graph
            .restore_research_branch(
                &RestoreResearchBranchRequest {
                    operation_uuid: Uuid::now_v7(),
                    expected_generation_uuid: self.generation(),
                    branch_uuid: self.a,
                    source_version_uuid: accepted.version,
                    version_uuid: restored,
                    created_at: 7,
                },
                &cancel(),
            )
            .unwrap();
        let before = self.generation();
        assert_eq!(
            self.graph
                .review_research_proposal(&accepted.review, &cancel())
                .unwrap(),
            accepted.receipt
        );
        assert_eq!(self.generation(), before);
        let reproposal = self.submit(restored, &["property:score"]);
        let review = self.review_request(reproposal, false);
        assert!(
            self.graph
                .review_research_proposal(&review, &cancel())
                .unwrap()
                .version_uuid
                .is_none()
        );
        for (branch, score) in [(self.a, 1), (self.b, 73)] {
            let view = self.graph.open_research_branch(branch).unwrap();
            assert_eq!(
                integer(
                    &view
                        .graph()
                        .execute("MATCH(n:Character) RETURN n.score AS score")
                        .unwrap(),
                    "score"
                ),
                score
            );
        }
        assert_eq!(
            integer(
                &self
                    .graph
                    .execute("MATCH(n:Character) RETURN n.score AS score")
                    .unwrap(),
                "score"
            ),
            99
        );
        let generation = self.generation();
        let mut changed = accepted.review.clone();
        changed.explanation.push_str(" changed");
        assert_eq!(
            self.graph
                .review_research_proposal(&changed, &cancel())
                .unwrap_err()
                .code(),
            "GF_IDEMPOTENCY_CONFLICT"
        );
        assert_eq!(self.generation(), generation);
        self.output.json("restoration-outcome",&json!({"branch_a_score":1,"branch_b_score":73,"parent_score":99,"review_replay_preserved":true,"reproposal_created_destination_version":false,"changed_request_error":"GF_IDEMPOTENCY_CONFLICT"}));
    }
}
