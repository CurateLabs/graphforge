@api @ontology @skip-node
Feature: Ontology API

  Scenario: load_ontology from a valid YAML file succeeds
    Given a valid ontology YAML file defining a Person label
    When I load the ontology from that file
    Then no error is raised

  Scenario: load_ontology from a valid JSON file succeeds
    Given a valid ontology JSON file defining a Paper label
    When I load the ontology from that file
    Then no error is raised

  Scenario: load_ontology raises an error for an invalid file
    Given a file containing invalid YAML
    When I load the ontology from that file
    Then an OntologyError is raised
