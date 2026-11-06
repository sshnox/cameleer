//! YAML step handlers for `saga`, `serviceCall`, `transformDataType`, `poll`, `kamelet`
//! and `script` (Camel 4 YAML DSL names). Route `inputType` / `outputType`
//! (`{urn, validate}`) are kept as route properties and applied by cameleer-core's route
//! contract hook.

use cameleer_core::processor::poll::{PollDefinition, DEFAULT_POLL_TIMEOUT_MS};
use cameleer_core::processor::saga::{SagaCompletionMode, SagaDefinition, SagaPropagation};
use cameleer_core::processor::script::ScriptDefinition;
use cameleer_core::processor::service_call::{DiscoveryConfig, FilterConfig, ServiceCallDefinition, ServiceDefinition, ServiceLoadBalancer};
use cameleer_core::processor::transformer::TransformDataTypeDefinition;
use cameleer_core::util::parse_duration;

use super::eips::{ref_name, string_list, unwrap_item, val_duration, val_i64, val_str};
use super::*;

/// Register the handlers (after the core EIP handlers, so `script` is replaced).
pub(super) fn register_extra_steps(r: &mut StepRegistry) {
    r.register("saga", load_saga);
    r.register("serviceCall", load_service_call);
    r.register("transformDataType", |ctx, cfg| {
        let to = ctx.required_str(cfg, &["toType"])?;
        let from = ctx.opt_str(cfg, "fromType");
        ctx.add(Box::new(TransformDataTypeDefinition::new(from, to)));
        Ok(())
    });
    r.register("poll", |ctx, cfg| {
        let uri = ctx.uri(cfg)?;
        let timeout = match cfg.get("timeout") {
            None | Some(Value::Null) => DEFAULT_POLL_TIMEOUT_MS,
            Some(v @ Value::String(_)) => {
                let timeout = val_str(ctx.camel(), v).map_err(|e| ctx.error(e.message))?;
                match timeout.trim().parse::<i64>() {
                    Ok(millis) => millis,
                    Err(_) => {
                        let duration = parse_duration(&timeout).map_err(|e| ctx.error(e.message))?;
                        i64::try_from(duration.as_millis()).map_err(|_| ctx.error("timeout exceeds i64 milliseconds"))?
                    }
                }
            }
            Some(v) => val_i64(ctx.camel(), v).map_err(|e| ctx.error(e.message))?,
        };
        ctx.add(Box::new(PollDefinition::new(uri, timeout)));
        Ok(())
    });
    r.register("kamelet", |ctx, cfg| {
        let name = match cfg {
            Value::String(s) => s.clone(),
            _ => ctx.required_str(cfg, &["name"])?,
        };
        let base = if name.starts_with("kamelet:") { name } else { format!("kamelet:{}", name) };
        let uri = build_uri(&base, cfg.get("parameters")).map_err(|e| ctx.error(e.message))?;
        ctx.add(Box::new(ToDefinition::new(uri)));
        Ok(())
    });
    r.register("script", |ctx, cfg| {
        let e = ctx.expression(cfg)?;
        ctx.add(Box::new(ScriptDefinition::new(e)));
        Ok(())
    });
}

fn endpoint_of(ctx: &StepContext<'_>, v: &Value) -> Result<String> {
    ctx.uri(v)
}

fn load_saga(ctx: &mut StepContext<'_>, cfg: &Value) -> Result<()> {
    let mut def = SagaDefinition::new();
    if let Some(p) = ctx.opt_str(cfg, "propagation") {
        def.propagation = SagaPropagation::parse(&val_str(ctx.camel(), &Value::String(p))?).map_err(|e| ctx.error(e.message))?;
    }
    if let Some(m) = ctx.opt_str(cfg, "completionMode") {
        def.completion_mode = SagaCompletionMode::parse(&val_str(ctx.camel(), &Value::String(m))?).map_err(|e| ctx.error(e.message))?;
    }
    if let Some(t) = cfg.get("timeout").filter(|v| !v.is_null()) {
        def.timeout = Some(val_duration(ctx.camel(), t, 1).map_err(|e| ctx.error(format!("timeout: {}", e.message)))?);
    }
    if let Some(c) = cfg.get("compensation").filter(|v| !v.is_null()) {
        def.compensation = Some(endpoint_of(ctx, c)?);
    }
    if let Some(c) = cfg.get("completion").filter(|v| !v.is_null()) {
        def.completion = Some(endpoint_of(ctx, c)?);
    }
    if let Some(s) = ctx.opt_str(cfg, "sagaService").or_else(|| ctx.opt_str(cfg, "sagaServiceRef")) {
        def.saga_service = Some(ref_name(&s));
    }
    let options: Vec<&Value> = match cfg.get("option") {
        Some(Value::Array(a)) => a.iter().collect(),
        Some(v @ Value::Object(_)) => vec![v],
        _ => Vec::new(),
    };
    for o in options {
        let o = unwrap_item(o, "option");
        let key = ctx.required_str(o, &["key", "name"])?;
        let e = ctx.expression(o)?;
        def.options.push((key, e));
    }
    ctx.add_block(Box::new(def), cfg.get("steps"))
}

fn servers_of(ctx: &StepContext<'_>, v: &Value) -> Result<Vec<ServiceDefinition>> {
    let list = match v {
        Value::Object(_) => string_list(v.get("servers")),
        other => string_list(Some(other)),
    };
    let mut out = Vec::new();
    for s in list {
        out.extend(ServiceDefinition::parse_list(&s).map_err(|e| ctx.error(e.message))?);
    }
    Ok(out)
}

fn load_service_call(ctx: &mut StepContext<'_>, cfg: &Value) -> Result<()> {
    let name = match cfg {
        Value::String(s) => s.clone(),
        _ => ctx.required_str(cfg, &["name"])?,
    };
    let mut def = ServiceCallDefinition::new(name);
    if let Some(u) = ctx.opt_str(cfg, "uri") {
        def.uri = Some(u);
    }
    if let Some(c) = ctx.opt_str(cfg, "component") {
        def.component = c;
    }
    if let Some(p) = ctx.opt_str(cfg, "pattern") {
        def.pattern = ExchangePattern::parse(&p).map_err(|e| ctx.error(e.message))?;
    }
    if let Some(e) = cfg.get("expression").filter(|v| !v.is_null()) {
        def.expression = Some(ctx.expression(e)?);
    }
    for key in ["staticServiceDiscovery", "combinedServiceDiscovery"] {
        if let Some(v) = cfg.get(key) {
            // combined discovery: {staticServiceDiscovery: [...]} nested entries
            let entries: Vec<&Value> = match v.get("staticServiceDiscovery") {
                Some(Value::Array(a)) => a.iter().collect(),
                Some(x) => vec![x],
                None => vec![v],
            };
            for e in entries {
                def.discovery.push(DiscoveryConfig::Static(servers_of(ctx, e)?));
            }
        }
    }
    if let Some(r) = ctx.opt_str(cfg, "serviceDiscoveryRef") {
        def.discovery.push(DiscoveryConfig::Ref(ref_name(&r)));
    }
    if let Some(v) = cfg.get("blacklistServiceFilter") {
        def.filters.push(FilterConfig::Blacklist(servers_of(ctx, v)?));
    }
    if cfg.get("healthyServiceFilter").is_some() {
        def.filters.push(FilterConfig::Healthy);
    }
    if cfg.get("passThroughServiceFilter").is_some() {
        def.filters.push(FilterConfig::PassThrough);
    }
    if cfg.get("randomLoadBalancer").is_some() {
        def.load_balancer = ServiceLoadBalancer::Random;
    }
    if cfg.get("roundRobinLoadBalancer").is_some() || cfg.get("defaultLoadBalancer").is_some() {
        def.load_balancer = ServiceLoadBalancer::RoundRobin;
    }
    ctx.add(Box::new(def));
    Ok(())
}
