//! YAML DSL: saga, serviceCall, transformDataType, route inputType/outputType, poll,
//! kamelet and script steps (plus the Rust DSL `kamelet` EIP).

use std::time::Duration;

use cameleer_core::prelude::*;
use cameleer_core::processor::poll::{PollDefinition, DEFAULT_POLL_TIMEOUT_MS};
use cameleer_core::processor::saga::SAGA_LONG_RUNNING_ACTION;
use cameleer_core::processor::transformer::{predicate_validator, processor_transformer, CamelContextTransformerExt};
use cameleer_yaml::*;

fn started(ctx: &CamelContext, yaml: &str) {
    add_routes_from_yaml(ctx, yaml).unwrap_or_else(|e| panic!("load failed: {}", e));
    ctx.start().unwrap();
}

fn root_kind(e: &CamelError) -> String {
    let mut cur = e;
    while cur.kind == "CamelExecutionException" {
        match &cur.cause {
            Some(c) => cur = &**c,
            None => break,
        }
    }
    cur.kind.clone()
}

fn headers(ctx: &CamelContext, mock: &str, h: &str) -> Vec<String> {
    ctx.mock_endpoint(mock).unwrap().received_exchanges().iter().map(|e| e.header(h).map(|v| v.to_string()).unwrap_or_default()).collect()
}

const SAGA: &str = r#"
- route:
    from:
      uri: direct:start
      steps:
        - saga:
            propagation: REQUIRED
            completionMode: AUTO
            compensation:
              uri: mock:compensate
            completion: mock:complete
            option:
              - key: id
                expression:
                  simple: "${header.id}"
              - key: step
                constant: outer
            steps:
              - to: direct:inner
              - filter:
                  simple: "${header.fail} == true"
                  steps:
                    - throwException:
                        exceptionType: java.io.IOException
                        message: boom
- route:
    from:
      uri: direct:inner
      steps:
        - saga:
            propagation: MANDATORY
            compensation: mock:compensate
            option:
              - key: step
                constant: inner
            steps:
              - to: mock:inner
- route:
    from:
      uri: direct:manual
      steps:
        - saga:
            completionMode: MANUAL
            timeout: 5s
            completion: mock:manualComplete
            steps:
              - to: mock:manual
- route:
    from:
      uri: direct:finish
      steps:
        - to: saga:complete
"#;

#[test]
fn yaml_saga_success_failure_and_manual_completion() {
    let ctx = CamelContext::new();
    started(&ctx, SAGA);
    let t = ctx.create_producer_template();
    t.request_body_and_header("direct:start", "x", "id", 1).unwrap();
    assert_eq!(headers(&ctx, "complete", "id"), vec!["1"]);
    assert_eq!(ctx.mock_endpoint("compensate").unwrap().received_counter(), 0);

    let e = t.request_body_and_headers("direct:start", "x", vec![("id", Value::from(2)), ("fail", Value::Bool(true))]).unwrap_err();
    assert_eq!(root_kind(&e), "IOException");
    assert_eq!(headers(&ctx, "compensate", "step"), vec!["inner", "outer"]);
    assert_eq!(headers(&ctx, "compensate", "id"), vec!["", "2"]);

    let ex = t.request("direct:manual", |ex| ex.set_body("m")).unwrap();
    assert_eq!(ctx.mock_endpoint("manualComplete").unwrap().received_counter(), 0);
    let lra = ex.header(SAGA_LONG_RUNNING_ACTION).unwrap().to_string();
    t.request_body_and_header("direct:finish", "", SAGA_LONG_RUNNING_ACTION, lra).unwrap();
    assert_eq!(ctx.mock_endpoint("manualComplete").unwrap().received_counter(), 1);
}

#[test]
fn yaml_saga_rejects_bad_propagation() {
    let ctx = CamelContext::new();
    let e = add_routes_from_yaml(&ctx, "- from:\n    uri: direct:a\n    steps:\n      - saga:\n          propagation: SOMETIMES\n").unwrap_err();
    assert!(e.message.contains("Unknown saga propagation"), "{}", e);
}

#[test]
fn yaml_service_call_round_robin_and_filters() {
    let ctx = CamelContext::new();
    started(
        &ctx,
        r#"
- from:
    uri: direct:a
    steps:
      - transform:
          simple: "a:${body}"
- from:
    uri: direct:b
    steps:
      - transform:
          simple: "b:${body}"
- from:
    uri: direct:start
    steps:
      - serviceCall:
          name: svc
          uri: "direct:{host}"
          staticServiceDiscovery:
            servers:
              - svc@a:8080
              - svc@b:8081
          roundRobinLoadBalancer: {}
- from:
    uri: direct:filtered
    steps:
      - serviceCall:
          name: svc
          expression:
            simple: "direct:${header.CamelServiceCallServiceHost}"
          staticServiceDiscovery:
            servers: "svc@a:8080,svc@b:8081"
          blacklistServiceFilter:
            servers:
              - svc@a:8080
          healthyServiceFilter: {}
"#,
    );
    let t = ctx.create_producer_template();
    let r: Vec<String> = (0..4).map(|i| t.request_body("direct:start", i).unwrap().to_string()).collect();
    assert_eq!(r, vec!["a:0", "b:1", "a:2", "b:3"]);
    assert_eq!(t.request_body("direct:filtered", "x").unwrap(), Value::from("b:x"));
    assert_eq!(t.request_body("direct:filtered", "y").unwrap(), Value::from("b:y"));
}

#[test]
fn yaml_route_input_output_types_and_transform_data_type() {
    let ctx = CamelContext::new();
    ctx.add_transformer(
        "json:Order",
        "xml:Order",
        processor_transformer(|ex: &mut Exchange| {
            let v = cameleer_core::json::parse(&ex.body_as::<String>()?)?;
            ex.set_body(format!("<order>{}</order>", v.get("id").map(|i| i.to_string()).unwrap_or_default()));
            Ok(())
        }),
    );
    ctx.add_validator("xml:Order", predicate_validator(body().starts_with("<order>")));
    started(
        &ctx,
        r#"
- route:
    id: json
    inputType:
      urn: json:Order
    from:
      uri: direct:json
      steps:
        - to: direct:xml
- route:
    id: xml
    inputType:
      urn: xml:Order
      validate: true
    outputType: text-plain
    from:
      uri: direct:xml
      steps:
        - to: mock:xml
- route:
    from:
      uri: direct:tdt
      steps:
        - transformDataType:
            fromType: json:Order
            toType: xml:Order
        - to: mock:tdt
        - transformDataType:
            toType: application-octet-stream
        - to: mock:bytes
"#,
    );
    let t = ctx.create_producer_template();
    let r = t.request_body("direct:json", r#"{"id":3}"#).unwrap();
    assert_eq!(r, Value::from("<order>3</order>"));
    assert_eq!(ctx.mock_endpoint("xml").unwrap().received_bodies(), vec![Value::from("<order>3</order>")]);
    // validation failure (sent directly with a wrong body)
    let e = t.request_body("direct:xml", "nope").unwrap_err();
    assert_eq!(root_kind(&e), kinds::VALIDATION, "{}", e);

    t.send_body("direct:tdt", r#"{"id":4}"#).unwrap();
    assert_eq!(ctx.mock_endpoint("tdt").unwrap().received_bodies(), vec![Value::from("<order>4</order>")]);
    assert_eq!(ctx.mock_endpoint("bytes").unwrap().received_bodies(), vec![Value::Bytes(b"<order>4</order>".to_vec())]);
}

#[test]
fn yaml_poll_timeout_accepts_duration_units() {
    let ctx = CamelContext::new();
    for (timeout, expected) in [("2s", 2000), ("500ms", 500), ("2000", 2000), ("-1", -1), ("null", DEFAULT_POLL_TIMEOUT_MS)] {
        let yaml = format!(
            "- from:\n    uri: direct:poll\n    steps:\n      - poll:\n          uri: seda:inbox\n          timeout: {timeout}\n"
        );
        let routes = load_routes(&ctx, &yaml).unwrap();
        let poll = routes[0].outputs[0].as_any().downcast_ref::<PollDefinition>().unwrap();
        assert_eq!(poll.timeout_ms, expected, "timeout: {timeout}");
    }
}

#[test]
fn yaml_poll_kamelet_and_script() {
    let ctx = CamelContext::new();
    started(
        &ctx,
        r#"
- from:
    uri: direct:poll
    steps:
      - poll:
          uri: seda:inbox
          timeout: 1000
      - to: mock:polled
- from:
    uri: direct:kamelet
    steps:
      - kamelet:
          name: set-body-action
          parameters:
            value: "hello ${body}"
      - to: mock:kamelet
- from:
    uri: direct:script
    steps:
      - script:
          simple: "${body.toUpperCase()}"
      - script:
          jq: ".a"
      - to: mock:script
"#,
    );
    let t = ctx.create_producer_template();
    t.send_body("seda:inbox", "mail").unwrap();
    t.send_body("direct:poll", "x").unwrap();
    assert_eq!(ctx.mock_endpoint("polled").unwrap().received_bodies(), vec![Value::from("mail")]);
    t.send_body("direct:kamelet", "world").unwrap();
    assert_eq!(ctx.mock_endpoint("kamelet").unwrap().received_bodies(), vec![Value::from("hello world")]);
    t.send_body("direct:script", r#"{"a":1}"#).unwrap();
    assert_eq!(ctx.mock_endpoint("script").unwrap().received_bodies(), vec![Value::from(r#"{"a":1}"#)]);
}

#[test]
fn rust_dsl_kamelet_eip() {
    let ctx = CamelContext::new();
    register_kamelet_component(&ctx);
    ctx.add_routes(|b| {
        b.from("direct:start").kamelet("set-body-action?value=hi").to("mock:out");
    })
    .unwrap();
    ctx.start().unwrap();
    ctx.create_producer_template().send_body("direct:start", "x").unwrap();
    let m = ctx.mock_endpoint("out").unwrap();
    m.expected_bodies_received(vec!["hi"]);
    m.assert_is_satisfied_within(Duration::from_secs(5)).unwrap();
}
