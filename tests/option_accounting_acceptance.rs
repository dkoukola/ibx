//! Deliberately destructive paper-only evidence capture for one long equity-option call.
//!
//! This is not part of the normal SDK test suite. Both entry points are ignored and
//! require exact semantic opt-ins, an exact expected paper account, an explicitly
//! selected option/underlying pair, bounded operator-supplied limit prices, and a
//! private durable JSONL receipt. The ordinary entry point buys at most one call contract,
//! physically reconnects, reconciles fresh broker history and positions, and closes
//! only the exposure established by its own order references. The recovery entry
//! point never creates an entry; it reloads either prior standard-share call or put
//! receipts, cancels receipt-owned working orders, freshly reconciles, and closes only
//! a corroborated receipt-owned exposure.
//!
//! Credentials are read from a private JSON file with exactly this shape:
//! `{"username":"...","password":"..."}`. They are never written to the receipt or
//! printed. Native wire capture is disabled during authentication and admits only the
//! reviewed incoming execution, commission, position, portfolio, account, and account
//! completion message classes after the authenticated account matches exactly.
//! A successful run additionally proves that every owned fill has a positive price at
//! or better than its owned limit, that the SDK derives premium cash with the
//! qualified 100 multiplier, and that every callback fee corresponds to a native
//! commission amount and currency actually present on the wire. Broker average cost
//! remains an opaque raw value, and derived fill cash is not called settled account
//! cash.
//!
//! Required environment for the entry test:
//! - `IB_OPTION_ACCOUNTING_OPT_IN=SEND_ONE_BOUNDED_PAPER_OPTION_AND_CLOSE_IT`
//! - `IB_OPTION_ACCOUNTING_CREDENTIAL_FILE=/absolute/private/credentials.json`
//! - `IB_EXPECTED_PAPER_ACCOUNT=<exact paper account>`
//! - `IB_OPTION_ACCOUNTING_RECEIPT=/absolute/private/new-receipt.jsonl`
//! - `IB_OPTION_ACCOUNTING_OPTION_CON_ID=<exact option conId>`
//! - `IB_OPTION_ACCOUNTING_UNDERLYING_CON_ID=<exact stock conId>`
//! - `IB_OPTION_ACCOUNTING_SYMBOL=<exact underlying symbol>`
//! - `IB_OPTION_ACCOUNTING_ENTRY_LIMIT=<positive limit no greater than 1.00>`
//! - `IB_OPTION_ACCOUNTING_CLOSE_LIMIT=<positive limit no greater than 1.00>`
//!
//! The recovery test instead requires
//! `IB_OPTION_ACCOUNTING_RECOVERY_OPT_IN=RECONCILE_AND_CLOSE_ONLY_RECEIPT_OWNED_PAPER_OPTION`
//! and opens the existing receipt for append. `IB_HOST` and
//! `IB_OPTION_ACCOUNTING_EXCHANGE` are optional (defaults: `cdc1.ibllc.com`, `SMART`).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::env;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crossbeam_channel::Sender;
use ibx::api::client::{Contract, EClient};
use ibx::api::types::{
    CommissionAndFeesReport, ContractDetails, Execution, ExecutionFilter, Order,
    OrderState as ApiOrderState, TickAttrib,
};
use ibx::api::wrapper::Wrapper;
use ibx::bridge::SharedState;
use ibx::control::contracts::SettlementMethod;
use ibx::gateway::{Gateway, GatewayConfig};
use ibx::protocol::order_write::OrderWriteOutcome;
use ibx::types::{
    ControlCommand, InstrumentId, OptionLifecycleObservation, OrderAttrs, OrderKind, OrderRequest,
    PRICE_SCALE, QTY_SCALE, RegistrationReply, Side,
};
use serde_json::{Value, json};
use zeroize::{Zeroize, Zeroizing};

const ENTRY_OPT_IN: &str = "SEND_ONE_BOUNDED_PAPER_OPTION_AND_CLOSE_IT";
const RECOVERY_OPT_IN: &str = "RECONCILE_AND_CLOSE_ONLY_RECEIPT_OWNED_PAPER_OPTION";
const SCHEMA: &str = "ibx-option-accounting-acceptance-v1";
const MAX_LIMIT: i64 = PRICE_SCALE;
const MIN_LIMIT: i64 = PRICE_SCALE / 100;
const REGISTRATION_TIMEOUT: Duration = Duration::from_secs(5);
const CALLBACK_TIMEOUT: Duration = Duration::from_secs(60);
const ORDER_TIMEOUT: Duration = Duration::from_secs(120);
const CANCEL_TIMEOUT: Duration = Duration::from_secs(30);
const OPTION_PREMIUM_MULTIPLIER: f64 = 100.0;

type Check<T> = Result<T, String>;

static LIVE_SERIAL: Mutex<()> = Mutex::new(());
static REQUEST_ID: AtomicI64 = AtomicI64::new(41_000);
static ORDER_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn now_micros() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
}

fn next_request_id() -> i64 {
    REQUEST_ID.fetch_add(1, Ordering::Relaxed)
}

fn new_order_ref(role: &str) -> String {
    let sequence = ORDER_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("oa-{:x}-{sequence:x}-{role}", now_micros())
}

fn env_required(name: &str) -> Check<String> {
    env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("{name} must be set explicitly"))
}

fn parse_positive_i64(name: &str, value: &str) -> Check<i64> {
    value
        .parse::<i64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("{name} must be a positive integer"))
}

fn parse_price(value: &str) -> Check<i64> {
    let value = value.trim();
    if value.is_empty() || value.starts_with(['+', '-']) {
        return Err("limit price must be an unsigned decimal".into());
    }
    let mut parts = value.split('.');
    let whole = parts.next().unwrap_or_default();
    let fraction = parts.next().unwrap_or_default();
    if parts.next().is_some()
        || whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.len() > 8
    {
        return Err("limit price must have at most eight decimal places".into());
    }
    let whole = whole
        .parse::<i64>()
        .map_err(|_| "limit price is too large".to_string())?;
    let mut fractional = if fraction.is_empty() {
        0
    } else {
        fraction
            .parse::<i64>()
            .map_err(|_| "invalid limit price fraction".to_string())?
    };
    for _ in fraction.len()..8 {
        fractional *= 10;
    }
    whole
        .checked_mul(PRICE_SCALE)
        .and_then(|scaled| scaled.checked_add(fractional))
        .filter(|scaled| *scaled >= MIN_LIMIT && *scaled <= MAX_LIMIT)
        .ok_or_else(|| "limit price must be between 0.01 and 1.00 inclusive".into())
}

fn private_metadata(path: &Path, kind: &str) -> Check<std::fs::Metadata> {
    if !path.is_absolute() {
        return Err(format!("{kind} path must be absolute"));
    }
    let metadata = path
        .metadata()
        .map_err(|_| format!("{kind} metadata is unavailable"))?;
    let process_uid = std::fs::metadata("/proc/self")
        .map_err(|_| "cannot determine the service user".to_string())?
        .uid();
    if metadata.uid() != process_uid || metadata.permissions().mode() & 0o077 != 0 {
        return Err(format!(
            "{kind} must be private and owned by the service user"
        ));
    }
    Ok(metadata)
}

struct Credentials {
    username: Zeroizing<String>,
    password: Zeroizing<String>,
}

impl Credentials {
    fn read(path: &Path) -> Check<Self> {
        let metadata = private_metadata(path, "credential file")?;
        if !metadata.is_file() {
            return Err("credential path must be a regular file".into());
        }
        let mut bytes = Zeroizing::new(Vec::new());
        File::open(path)
            .map_err(|_| "credential file cannot be opened".to_string())?
            .take(16 * 1024 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| "credential file cannot be read".to_string())?;
        if bytes.len() > 16 * 1024 {
            return Err("credential file is too large".into());
        }
        let value: Value = serde_json::from_slice(&bytes)
            .map_err(|_| "credential file is not valid JSON".to_string())?;
        let object = value
            .as_object()
            .filter(|object| object.len() == 2)
            .ok_or_else(|| "credential file must contain only username and password".to_string())?;
        let username = object
            .get("username")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "credential username is missing".to_string())?;
        let password = object
            .get("password")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "credential password is missing".to_string())?;
        Ok(Self {
            username: Zeroizing::new(username.to_string()),
            password: Zeroizing::new(password.to_string()),
        })
    }

    fn gateway_config(&self, host: &str) -> GatewayConfig {
        GatewayConfig {
            username: self.username.to_string(),
            password: Zeroizing::new(self.password.to_string()),
            host: host.to_string(),
            paper: true,
            accept_invalid_certs: false,
            ib_key_timeout_secs: ibx::auth::session::IB_KEY_DEFAULT_TIMEOUT_SECS,
            ib_key_token_sub_type: ibx::auth::session::IB_KEY_DEFAULT_TOKEN_SUB_TYPE.into(),
            code_provider: None,
        }
    }
}

struct Receipt {
    file: Mutex<File>,
    failure: Mutex<Option<String>>,
}

impl Receipt {
    fn create(path: &Path) -> Check<Arc<Self>> {
        let parent = path
            .parent()
            .ok_or_else(|| "receipt requires a parent directory".to_string())?;
        let parent_metadata = private_metadata(parent, "receipt directory")?;
        if !parent_metadata.is_dir() {
            return Err("receipt parent is not a directory".into());
        }
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .map_err(|error| format!("cannot create receipt: {error}"))?;
        Ok(Arc::new(Self {
            file: Mutex::new(file),
            failure: Mutex::new(None),
        }))
    }

    fn open_append(path: &Path) -> Check<Arc<Self>> {
        let metadata = private_metadata(path, "receipt")?;
        if !metadata.is_file() {
            return Err("receipt path is not a regular file".into());
        }
        let file = OpenOptions::new()
            .append(true)
            .open(path)
            .map_err(|error| format!("cannot append receipt: {error}"))?;
        Ok(Arc::new(Self {
            file: Mutex::new(file),
            failure: Mutex::new(None),
        }))
    }

    fn append(&self, kind: &str, stage: &str, evidence: Value) {
        if self.failure.lock().unwrap().is_some() {
            return;
        }
        let result = (|| -> Result<(), Box<dyn std::error::Error>> {
            let mut file = self.file.lock().unwrap();
            serde_json::to_writer(
                &mut *file,
                &json!({
                    "schema": SCHEMA,
                    "recorded_at_unix_micros": now_micros().to_string(),
                    "kind": kind,
                    "stage": stage,
                    "evidence": evidence,
                }),
            )?;
            file.write_all(b"\n")?;
            file.sync_all()?;
            Ok(())
        })();
        if let Err(error) = result {
            *self.failure.lock().unwrap() = Some(error.to_string());
        }
    }

    fn check(&self) -> Check<()> {
        self.failure
            .lock()
            .unwrap()
            .clone()
            .map_or(Ok(()), |error| {
                Err(format!("receipt write failed: {error}"))
            })
    }
}

#[derive(Clone, Debug)]
struct NativePortfolioRow {
    session: u64,
    sequence: u64,
    account_request: String,
    con_id: i64,
    quantity: Option<String>,
    average_cost: Option<String>,
    market_price: Option<String>,
    market_value: Option<String>,
    unrealized_pnl: Option<String>,
    realized_pnl: Option<String>,
}

#[derive(Clone, Debug)]
struct NativeExecution {
    session: u64,
    sequence: u64,
    execution_id: String,
    order_ref: Option<String>,
    correction_status: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct NativeCommission {
    session: u64,
    sequence: u64,
    execution_id: String,
    amount: Option<String>,
    currency: Option<String>,
}

#[derive(Default)]
struct NativeTracker {
    sequence: u64,
    account_frames: Vec<(u64, u64, String, String)>,
    account_ends: Vec<(u64, u64, String)>,
    position_completions: Vec<(u64, u64, String)>,
    portfolio_rows: Vec<NativePortfolioRow>,
    option_execution_frames: Vec<NativeExecution>,
    commissions: Vec<NativeCommission>,
}

struct WireCapture {
    active: AtomicBool,
    session: AtomicU64,
    option_con_id: AtomicI64,
    underlying_con_id: AtomicI64,
    expected_account: Mutex<String>,
    account_request: Mutex<String>,
    stage: Mutex<String>,
    receipt: Mutex<Option<Arc<Receipt>>>,
    tracker: Mutex<NativeTracker>,
}

impl WireCapture {
    fn new() -> Self {
        Self {
            active: AtomicBool::new(false),
            session: AtomicU64::new(0),
            option_con_id: AtomicI64::new(0),
            underlying_con_id: AtomicI64::new(0),
            expected_account: Mutex::new(String::new()),
            account_request: Mutex::new(String::new()),
            stage: Mutex::new(String::new()),
            receipt: Mutex::new(None),
            tracker: Mutex::new(NativeTracker::default()),
        }
    }

    fn arm(
        &self,
        receipt: Arc<Receipt>,
        expected_account: &str,
        option_con_id: i64,
        underlying_con_id: i64,
        stage: &str,
    ) -> u64 {
        self.active.store(false, Ordering::Release);
        *self.receipt.lock().unwrap() = Some(receipt);
        *self.expected_account.lock().unwrap() = expected_account.to_string();
        self.account_request.lock().unwrap().clear();
        *self.stage.lock().unwrap() = stage.to_string();
        self.option_con_id.store(option_con_id, Ordering::Release);
        self.underlying_con_id
            .store(underlying_con_id, Ordering::Release);
        let session = self.session.fetch_add(1, Ordering::AcqRel) + 1;
        self.active.store(true, Ordering::Release);
        session
    }

    fn disarm(&self) {
        self.active.store(false, Ordering::Release);
    }

    fn set_stage(&self, stage: &str) {
        *self.stage.lock().unwrap() = stage.to_string();
    }

    fn set_account_request(&self, request: &str) {
        *self.account_request.lock().unwrap() = request.to_string();
    }

    fn marker(&self) -> (u64, u64, usize, usize, usize) {
        let tracker = self.tracker.lock().unwrap();
        (
            self.session.load(Ordering::Acquire),
            tracker.sequence,
            tracker.account_frames.len(),
            tracker.portfolio_rows.len(),
            tracker.option_execution_frames.len(),
        )
    }

    fn commission_marker(&self) -> (u64, u64, usize) {
        let tracker = self.tracker.lock().unwrap();
        (
            self.session.load(Ordering::Acquire),
            tracker.sequence,
            tracker.commissions.len(),
        )
    }

    fn commissions_since(&self, marker: (u64, u64, usize)) -> Vec<NativeCommission> {
        let (session, sequence, index) = marker;
        self.tracker.lock().unwrap().commissions[index..]
            .iter()
            .filter(|row| row.session == session && row.sequence > sequence)
            .cloned()
            .collect()
    }

    fn has_connection_image(
        &self,
        session: u64,
        expected_account: &str,
        account_request: &str,
    ) -> bool {
        let tracker = self.tracker.lock().unwrap();
        let has_account_end = tracker
            .account_ends
            .iter()
            .any(|(row_session, _, request)| *row_session == session && request == account_request);
        has_account_end
            && tracker
                .position_completions
                .iter()
                .any(|(row_session, _, account)| {
                    *row_session == session && account == expected_account
                })
    }

    fn post_fill_evidence(
        &self,
        marker: (u64, u64, usize, usize, usize),
        expected_quantity: f64,
        execution_ids: &BTreeSet<String>,
        owned_order_ref: &str,
    ) -> bool {
        let (session, sequence, account_index, portfolio_index, execution_index) = marker;
        let account_request = self.account_request.lock().unwrap().clone();
        let tracker = self.tracker.lock().unwrap();
        let account = tracker.account_frames[account_index..].iter().any(
            |(row_session, row_sequence, _, request)| {
                *row_session == session && *row_sequence > sequence && request == &account_request
            },
        );
        let execution = !execution_ids.is_empty()
            && execution_ids.iter().all(|execution_id| {
                tracker.option_execution_frames[execution_index..]
                    .iter()
                    .any(|row| {
                        row.session == session
                            && row.sequence > sequence
                            && row.execution_id == *execution_id
                            && !row.correction_status
                            && row
                                .order_ref
                                .as_deref()
                                .is_none_or(|value| value == owned_order_ref)
                    })
            });
        let portfolio = tracker.portfolio_rows[portfolio_index..].iter().any(|row| {
            row.session == session
                && row.sequence > sequence
                && row.account_request == account_request
                && row.con_id == self.option_con_id.load(Ordering::Acquire)
                && row
                    .quantity
                    .as_deref()
                    .and_then(|value| value.parse::<f64>().ok())
                    == Some(expected_quantity)
                && row.average_cost.is_some()
        });
        account && execution && portfolio
    }

    fn post_fill_portfolio_row(
        &self,
        marker: (u64, u64, usize, usize, usize),
        expected_quantity: f64,
    ) -> Option<NativePortfolioRow> {
        let (session, sequence, _, portfolio_index, _) = marker;
        let account_request = self.account_request.lock().unwrap().clone();
        let tracker = self.tracker.lock().unwrap();
        tracker.portfolio_rows[portfolio_index..]
            .iter()
            .rev()
            .find(|row| {
                row.session == session
                    && row.sequence > sequence
                    && row.account_request == account_request
                    && row.con_id == self.option_con_id.load(Ordering::Acquire)
                    && row
                        .quantity
                        .as_deref()
                        .and_then(|value| value.parse::<f64>().ok())
                        == Some(expected_quantity)
            })
            .cloned()
    }

    fn note(&self, fields: &[(u32, String)]) {
        let session = self.session.load(Ordering::Acquire);
        let msg_type = field(fields, 35).unwrap_or_default();
        let mut tracker = self.tracker.lock().unwrap();
        tracker.sequence += 1;
        let sequence = tracker.sequence;
        match msg_type {
            "UT" | "UM" | "RL" => {
                tracker.account_frames.push((
                    session,
                    sequence,
                    msg_type.to_string(),
                    field(fields, 6529).unwrap_or_default().to_string(),
                ));
            }
            "EB" => tracker.account_ends.push((
                session,
                sequence,
                field(fields, 6529).unwrap_or_default().to_string(),
            )),
            "8" => {
                let expected_account = self.expected_account.lock().unwrap().clone();
                if let Some((execution_id, order_ref, correction_status)) =
                    ordinary_native_execution(
                        fields,
                        self.option_con_id.load(Ordering::Acquire),
                        &expected_account,
                    )
                {
                    tracker.option_execution_frames.push(NativeExecution {
                        session,
                        sequence,
                        execution_id,
                        order_ref,
                        correction_status,
                    });
                }
            }
            "U" if field(fields, 6040) == Some("60") => {
                tracker.commissions.push(NativeCommission {
                    session,
                    sequence,
                    execution_id: field(fields, 17).unwrap_or_default().to_string(),
                    amount: field(fields, 6378).map(str::to_string),
                    currency: field(fields, 6381).map(str::to_string),
                });
            }
            "U" if field(fields, 6040) == Some("75") => {
                let expected_account = self.expected_account.lock().unwrap().clone();
                for account in position_completion_accounts(fields) {
                    if account == expected_account {
                        tracker
                            .position_completions
                            .push((session, sequence, account));
                    }
                }
            }
            "UP" => {
                tracker.portfolio_rows.extend(portfolio_rows(
                    fields,
                    session,
                    sequence,
                    self.option_con_id.load(Ordering::Acquire),
                    self.underlying_con_id.load(Ordering::Acquire),
                ));
            }
            _ => {}
        }
    }
}

impl log::Log for WireCapture {
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        if !self.active.load(Ordering::Acquire) {
            return;
        }
        let message = record.args().to_string();
        if !message.starts_with("WIRE<") {
            return;
        }
        let fields = parse_wire_fields(&message);
        if !allowed_incoming(&fields, self.option_con_id.load(Ordering::Acquire)) {
            return;
        }
        self.note(&fields);
        let Some(receipt) = self.receipt.lock().unwrap().clone() else {
            return;
        };
        let stage = self.stage.lock().unwrap().clone();
        let source = message
            .strip_prefix("WIRE< ")
            .and_then(|rest| rest.split_once(' '))
            .map(|(source, _)| source)
            .unwrap_or("unknown");
        receipt.append(
            "native_incoming",
            &stage,
            json!({
                "session": self.session.load(Ordering::Acquire),
                "source": source,
                "fields": fields
                    .iter()
                    .map(|(tag, value)| json!([tag, value]))
                    .collect::<Vec<_>>(),
            }),
        );
    }

    fn flush(&self) {}
}

fn wire_capture() -> &'static WireCapture {
    static CAPTURE: OnceLock<&'static WireCapture> = OnceLock::new();
    CAPTURE.get_or_init(|| {
        let capture: &'static WireCapture = Box::leak(Box::new(WireCapture::new()));
        log::set_logger(capture).expect("option accounting capture must own the test logger");
        log::set_max_level(log::LevelFilter::Trace);
        capture
    })
}

fn parse_wire_fields(line: &str) -> Vec<(u32, String)> {
    let body = line.find("8=").map(|index| &line[index..]).unwrap_or("");
    body.split('|')
        .filter_map(|part| {
            let (tag, value) = part.split_once('=')?;
            Some((tag.parse().ok()?, value.to_string()))
        })
        .collect()
}

fn field(fields: &[(u32, String)], tag: u32) -> Option<&str> {
    fields
        .iter()
        .find(|(candidate, _)| *candidate == tag)
        .map(|(_, value)| value.as_str())
}

fn allowed_incoming(fields: &[(u32, String)], option_con_id: i64) -> bool {
    match field(fields, 35) {
        Some("UT" | "UM" | "RL" | "UP" | "EB") => true,
        Some("U") => matches!(field(fields, 6040), Some("60" | "75")),
        Some("8") => {
            field(fields, 6008).and_then(|value| value.parse::<i64>().ok()) == Some(option_con_id)
                || field(fields, 32) == Some("*")
        }
        _ => false,
    }
}

fn ordinary_native_execution(
    fields: &[(u32, String)],
    option_con_id: i64,
    expected_account: &str,
) -> Option<(String, Option<String>, bool)> {
    let ordinary = field(fields, 6008).and_then(|value| value.parse::<i64>().ok())
        == Some(option_con_id)
        && matches!(field(fields, 150), Some("F" | "1" | "2"))
        && field(fields, 20) != Some("3")
        && matches!(field(fields, 6809), None | Some("-1"))
        && field(fields, 1).is_none_or(|account| account == expected_account)
        && field(fields, 32)
            .and_then(|value| value.parse::<f64>().ok())
            .is_some_and(|value| value.is_finite() && value > 0.0);
    if !ordinary {
        return None;
    }
    Some((
        field(fields, 17)?.to_string(),
        field(fields, 6010).map(str::to_string),
        matches!(field(fields, 20), Some("1" | "2")),
    ))
}

fn position_completion_accounts(fields: &[(u32, String)]) -> Vec<String> {
    let mut accounts = Vec::new();
    let mut account = None;
    for (tag, value) in fields {
        match *tag {
            1 => account = Some(value.clone()),
            6544 if value == "1" => {
                if let Some(account) = account.take() {
                    accounts.push(account);
                }
            }
            _ => {}
        }
    }
    accounts
}

fn portfolio_rows(
    fields: &[(u32, String)],
    session: u64,
    sequence: u64,
    option_con_id: i64,
    underlying_con_id: i64,
) -> Vec<NativePortfolioRow> {
    let mut rows = Vec::new();
    let mut row: BTreeMap<u32, String> = BTreeMap::new();
    let finish = |rows: &mut Vec<NativePortfolioRow>, row: &mut BTreeMap<u32, String>| {
        let con_id = row.get(&6008).and_then(|value| value.parse::<i64>().ok());
        if con_id.is_some_and(|value| value == option_con_id || value == underlying_con_id) {
            rows.push(NativePortfolioRow {
                session,
                sequence,
                account_request: field(fields, 6529).unwrap_or_default().to_string(),
                con_id: con_id.unwrap(),
                quantity: row.get(&6064).cloned(),
                average_cost: row.get(&6101).cloned(),
                market_price: row.get(&6065).cloned(),
                market_value: row.get(&6067).cloned(),
                unrealized_pnl: row.get(&6100).cloned(),
                realized_pnl: row.get(&6099).cloned(),
            });
        }
        row.clear();
    };
    if !fields.iter().any(|(tag, _)| *tag == 6068) {
        row.extend(fields.iter().cloned());
        finish(&mut rows, &mut row);
        return rows;
    }
    for (tag, value) in fields {
        if *tag == 6068 && !row.is_empty() {
            finish(&mut rows, &mut row);
        }
        if !row.is_empty() || *tag == 6068 {
            row.insert(*tag, value.clone());
        }
    }
    if !row.is_empty() {
        finish(&mut rows, &mut row);
    }
    rows
}

#[derive(Clone)]
struct ConnectionSettings {
    credential_path: PathBuf,
    expected_account: String,
    host: String,
}

impl ConnectionSettings {
    fn from_env() -> Check<Self> {
        let credential_path = PathBuf::from(env_required("IB_OPTION_ACCOUNTING_CREDENTIAL_FILE")?);
        let expected_account = env_required("IB_EXPECTED_PAPER_ACCOUNT")?;
        if !expected_account.starts_with("DU") {
            return Err("expected account must be an explicit paper account".into());
        }
        Ok(Self {
            credential_path,
            expected_account,
            host: env::var("IB_HOST").unwrap_or_else(|_| "cdc1.ibllc.com".into()),
        })
    }
}

#[derive(Clone)]
struct FreshSnapshot {
    native_session: u64,
    execution_history: String,
    account_request: String,
    option_position_fixed: i64,
    option_average_cost: i64,
    underlying_position_fixed: i64,
    underlying_average_cost: i64,
}

#[derive(Clone)]
struct Plan {
    expected_account: String,
    option_con_id: i64,
    underlying_con_id: i64,
    symbol: String,
    exchange: String,
    entry_limit: i64,
    close_limit: i64,
    entry_order_ref: String,
    history_start: String,
    baseline_option_fixed: Option<i64>,
    baseline_underlying_fixed: Option<i64>,
    owned_order_refs: BTreeSet<String>,
    known_execution_ids: BTreeSet<String>,
}

impl Plan {
    fn from_entry_env(settings: &ConnectionSettings) -> Check<Self> {
        if env_required("IB_OPTION_ACCOUNTING_OPT_IN")? != ENTRY_OPT_IN {
            return Err("exact option-accounting entry opt-in is required".into());
        }
        let option_con_id = parse_positive_i64(
            "IB_OPTION_ACCOUNTING_OPTION_CON_ID",
            &env_required("IB_OPTION_ACCOUNTING_OPTION_CON_ID")?,
        )?;
        let underlying_con_id = parse_positive_i64(
            "IB_OPTION_ACCOUNTING_UNDERLYING_CON_ID",
            &env_required("IB_OPTION_ACCOUNTING_UNDERLYING_CON_ID")?,
        )?;
        if option_con_id == underlying_con_id {
            return Err("option and underlying conIds must differ".into());
        }
        let symbol = env_required("IB_OPTION_ACCOUNTING_SYMBOL")?;
        if !symbol
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
        {
            return Err("option-accounting symbol contains unsupported characters".into());
        }
        let exchange = env::var("IB_OPTION_ACCOUNTING_EXCHANGE")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "SMART".into());
        let entry_limit = parse_price(&env_required("IB_OPTION_ACCOUNTING_ENTRY_LIMIT")?)?;
        let close_limit = parse_price(&env_required("IB_OPTION_ACCOUNTING_CLOSE_LIMIT")?)?;
        let entry_order_ref = new_order_ref("entry");
        let history_start = format!("{}-00:00:00", &ibx::gateway::chrono_free_timestamp()[..8]);
        let mut owned_order_refs = BTreeSet::new();
        owned_order_refs.insert(entry_order_ref.clone());
        Ok(Self {
            expected_account: settings.expected_account.clone(),
            option_con_id,
            underlying_con_id,
            symbol,
            exchange,
            entry_limit,
            close_limit,
            entry_order_ref,
            history_start,
            baseline_option_fixed: None,
            baseline_underlying_fixed: None,
            owned_order_refs,
            known_execution_ids: BTreeSet::new(),
        })
    }

    fn record_header(&self, receipt: &Receipt) {
        receipt.append(
            "plan",
            "configured",
            json!({
                "expected_account": self.expected_account,
                "option_con_id": self.option_con_id,
                "underlying_con_id": self.underlying_con_id,
                "symbol": self.symbol,
                "exchange": self.exchange,
                "entry_limit_raw_price_scale": self.entry_limit.to_string(),
                "close_limit_raw_price_scale": self.close_limit.to_string(),
                "entry_order_ref": self.entry_order_ref,
                "history_start_utc": self.history_start,
                "quantity_request": "one whole native order unit",
                "economic_unit_interpretation": null,
            }),
        );
    }

    fn record_baseline(&mut self, image: &FreshSnapshot, receipt: &Receipt) -> Check<()> {
        if image.option_position_fixed != 0 {
            return Err("selected option is not flat in the fresh native baseline".into());
        }
        self.baseline_option_fixed = Some(image.option_position_fixed);
        self.baseline_underlying_fixed = Some(image.underlying_position_fixed);
        receipt.append(
            "baseline",
            "before_entry",
            json!({
                "native_session": image.native_session,
                "execution_history": image.execution_history,
                "account_request": image.account_request,
                "option_position_fixed_qty_scale": image.option_position_fixed.to_string(),
                "option_average_cost_raw_price_scale": image.option_average_cost.to_string(),
                "underlying_position_fixed_qty_scale": image.underlying_position_fixed.to_string(),
                "underlying_average_cost_raw_price_scale": image.underlying_average_cost.to_string(),
                "source": "SDK production parser on a completed physical-connection image; local subscription callbacks corroborated separately",
            }),
        );
        Ok(())
    }
}

fn value_string(value: &Value, key: &str) -> Check<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("receipt plan is missing {key}"))
}

fn value_i64(value: &Value, key: &str) -> Check<i64> {
    value
        .get(key)
        .and_then(Value::as_i64)
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("receipt plan has invalid {key}"))
}

fn recorded_owned_execution_id(
    evidence: &Value,
    owned_order_refs: &BTreeSet<String>,
    expected_account: &str,
    option_con_id: i64,
) -> Check<Option<String>> {
    let Some(execution) = evidence.get("execution") else {
        return Ok(None);
    };
    let order_ref = execution
        .get("order_ref")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !owned_order_refs.contains(order_ref) {
        return Ok(None);
    }
    let execution_id = execution
        .get("exec_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if execution_id.is_empty()
        || execution.get("account").and_then(Value::as_str) != Some(expected_account)
        || evidence
            .get("contract")
            .and_then(|contract| contract.get("con_id"))
            .and_then(Value::as_i64)
            != Some(option_con_id)
        || execution
            .get("pending_price_revision")
            .and_then(Value::as_bool)
            != Some(false)
    {
        return Err("receipt contains a contradictory owned execution record".into());
    }
    Ok(Some(execution_id.to_string()))
}

fn read_plan(path: &Path, expected_account: &str, close_limit: i64) -> Check<Plan> {
    let metadata = private_metadata(path, "receipt")?;
    if !metadata.is_file() {
        return Err("receipt path is not a regular file".into());
    }
    let file = File::open(path).map_err(|_| "receipt cannot be opened".to_string())?;
    let mut plan_value = None;
    let mut baseline_option_fixed = None;
    let mut baseline_underlying_fixed = None;
    let mut owned_order_refs = BTreeSet::new();
    let mut recorded_executions = Vec::new();
    let mut recorded_history_ids = BTreeSet::new();
    for line in BufReader::new(file).lines() {
        let mut line = Zeroizing::new(line.map_err(|_| "receipt cannot be read".to_string())?);
        let value: Value =
            serde_json::from_str(&line).map_err(|_| "receipt contains invalid JSON".to_string())?;
        line.zeroize();
        if value.get("schema").and_then(Value::as_str) != Some(SCHEMA) {
            return Err("receipt schema is not recognized".into());
        }
        let kind = value
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let evidence = value.get("evidence").unwrap_or(&Value::Null);
        match kind {
            "plan" if plan_value.is_none() => plan_value = Some(evidence.clone()),
            "baseline" => {
                baseline_option_fixed = evidence
                    .get("option_position_fixed_qty_scale")
                    .and_then(Value::as_str)
                    .and_then(|value| value.parse::<i64>().ok());
                baseline_underlying_fixed = evidence
                    .get("underlying_position_fixed_qty_scale")
                    .and_then(Value::as_str)
                    .and_then(|value| value.parse::<i64>().ok());
            }
            "order_intent" => {
                if let Some(order_ref) = evidence.get("order_ref").and_then(Value::as_str) {
                    owned_order_refs.insert(order_ref.to_string());
                }
            }
            "callback_execution" => recorded_executions.push(evidence.clone()),
            "history_reconciliation" => {
                if let Some(ids) = evidence
                    .get("owned_execution_ids")
                    .and_then(Value::as_array)
                {
                    for execution_id in ids.iter().filter_map(Value::as_str) {
                        if !execution_id.is_empty() {
                            recorded_history_ids.insert(execution_id.to_string());
                        }
                    }
                }
            }
            _ => {}
        }
    }
    let value = plan_value.ok_or_else(|| "receipt has no plan record".to_string())?;
    if value_string(&value, "expected_account")? != expected_account {
        return Err("receipt account does not match the explicit expected account".into());
    }
    let entry_order_ref = value_string(&value, "entry_order_ref")?;
    owned_order_refs.insert(entry_order_ref.clone());
    let option_con_id = value_i64(&value, "option_con_id")?;
    let mut known_execution_ids = recorded_history_ids;
    for evidence in recorded_executions {
        if let Some(execution_id) = recorded_owned_execution_id(
            &evidence,
            &owned_order_refs,
            expected_account,
            option_con_id,
        )? {
            known_execution_ids.insert(execution_id);
        }
    }
    Ok(Plan {
        expected_account: expected_account.to_string(),
        option_con_id,
        underlying_con_id: value_i64(&value, "underlying_con_id")?,
        symbol: value_string(&value, "symbol")?,
        exchange: value_string(&value, "exchange")?,
        entry_limit: value_string(&value, "entry_limit_raw_price_scale")?
            .parse()
            .map_err(|_| "receipt entry limit is invalid".to_string())?,
        close_limit,
        entry_order_ref,
        history_start: value_string(&value, "history_start_utc")?,
        baseline_option_fixed,
        baseline_underlying_fixed,
        owned_order_refs,
        known_execution_ids,
    })
}

fn contract_value(contract: &Contract) -> Value {
    json!({
        "con_id": contract.con_id,
        "symbol": contract.symbol,
        "sec_type": contract.sec_type,
        "exchange": contract.exchange,
        "primary_exchange": contract.primary_exchange,
        "currency": contract.currency,
        "last_trade_date_or_contract_month": contract.last_trade_date_or_contract_month,
        "strike": contract.strike.to_string(),
        "right": contract.right,
        "multiplier": contract.multiplier,
        "local_symbol": contract.local_symbol,
        "trading_class": contract.trading_class,
    })
}

fn details_value(details: &ContractDetails) -> Value {
    json!({
        "contract": contract_value(&details.contract),
        "order_types": details.order_types,
        "valid_exchanges": details.valid_exchanges,
        "under_con_id": details.under_con_id,
        "under_symbol": details.under_symbol,
        "under_sec_type": details.under_sec_type,
        "contract_month": details.contract_month,
        "real_expiration_date": details.real_expiration_date,
        "last_trade_time": details.last_trade_time,
        "time_zone_id": details.time_zone_id,
        "exercise_style": details.exercise_style,
        "settlement_method": format!("{:?}", details.settlement_method),
        "basket_composition": details.basket_composition,
    })
}

#[derive(Clone)]
struct SeenExecution {
    req_id: i64,
    contract: Contract,
    execution: Execution,
}

#[derive(Clone)]
struct SeenOpenOrder {
    order_id: i64,
    contract: Contract,
    order_ref: String,
    action: String,
    account: String,
}

#[derive(Default)]
struct Observed {
    details: Vec<(i64, ContractDetails)>,
    detail_ends: BTreeSet<i64>,
    executions: Vec<SeenExecution>,
    execution_ends: BTreeSet<i64>,
    commissions: Vec<CommissionAndFeesReport>,
    positions: Vec<(String, Contract, f64, f64)>,
    position_ends: usize,
    account_end_accounts: Vec<String>,
    open_orders: Vec<SeenOpenOrder>,
    open_order_ends: usize,
    order_status: HashMap<i64, String>,
    errors: Vec<(i64, i64, String)>,
    lifecycle: Vec<OptionLifecycleObservation>,
    bid: Option<(i64, i32, f64)>,
    ask: Option<(i64, i32, f64)>,
    market_data_types: Vec<(i64, i32)>,
    snapshot_ends: BTreeSet<i64>,
    connection_closed: bool,
}

struct Probe {
    receipt: Arc<Receipt>,
    observed: Observed,
}

impl Probe {
    fn new(receipt: Arc<Receipt>) -> Self {
        Self {
            receipt,
            observed: Observed::default(),
        }
    }

    fn record(&self, kind: &str, evidence: Value) {
        let stage = wire_capture().stage.lock().unwrap().clone();
        self.receipt.append(kind, &stage, evidence);
    }

    fn reject_lifecycle(&self) -> Check<()> {
        if self.observed.lifecycle.is_empty() {
            Ok(())
        } else {
            Err("non-default option lifecycle observation makes ordinary-fill accounting unsupported".into())
        }
    }
}

impl Wrapper for Probe {
    fn error(&mut self, req_id: i64, code: i64, message: &str, _: &str) {
        self.record(
            "callback_error",
            json!({"request_id": req_id, "code": code, "message": message}),
        );
        self.observed.errors.push((req_id, code, message.into()));
    }

    fn tick_price(&mut self, req_id: i64, tick_type: i32, price: f64, attrib: &TickAttrib) {
        self.record(
            "callback_tick_price",
            json!({
                "request_id": req_id,
                "tick_type": tick_type,
                "value": price.to_string(),
                "can_auto_execute": attrib.can_auto_execute,
                "past_limit": attrib.past_limit,
                "pre_open": attrib.pre_open,
                "freshness_claimed": false,
            }),
        );
        match tick_type {
            1 | 66 => self.observed.bid = Some((req_id, tick_type, price)),
            2 | 67 => self.observed.ask = Some((req_id, tick_type, price)),
            _ => {}
        }
    }

    fn market_data_type(&mut self, req_id: i64, market_data_type: i32) {
        self.record(
            "callback_market_data_type",
            json!({"request_id": req_id, "market_data_type": market_data_type}),
        );
        self.observed
            .market_data_types
            .push((req_id, market_data_type));
    }

    fn tick_snapshot_end(&mut self, req_id: i64) {
        self.record("callback_tick_snapshot_end", json!({"request_id": req_id}));
        self.observed.snapshot_ends.insert(req_id);
    }

    fn contract_details(&mut self, req_id: i64, details: &ContractDetails) {
        self.record(
            "callback_contract_details",
            json!({"request_id": req_id, "details": details_value(details)}),
        );
        self.observed.details.push((req_id, details.clone()));
    }

    fn contract_details_end(&mut self, req_id: i64) {
        self.record(
            "callback_contract_details_end",
            json!({"request_id": req_id}),
        );
        self.observed.detail_ends.insert(req_id);
    }

    fn exec_details(&mut self, req_id: i64, contract: &Contract, execution: &Execution) {
        self.record(
            "callback_execution",
            json!({
                "request_id": req_id,
                "contract": contract_value(contract),
                "execution": {
                    "exec_id": execution.exec_id,
                    "time": execution.time,
                    "account": execution.acct_number,
                    "exchange": execution.exchange,
                    "side": execution.side,
                    "shares": execution.shares.to_string(),
                    "price": execution.price.to_string(),
                    "perm_id": execution.perm_id,
                    "client_id": execution.client_id,
                    "order_id": execution.order_id,
                    "cum_qty": execution.cum_qty.to_string(),
                    "avg_price": execution.avg_price.to_string(),
                    "liquidation": execution.liquidation,
                    "order_ref": execution.order_ref,
                    "pending_price_revision": execution.pending_price_revision,
                },
            }),
        );
        self.observed.executions.push(SeenExecution {
            req_id,
            contract: contract.clone(),
            execution: execution.clone(),
        });
    }

    fn exec_details_end(&mut self, req_id: i64) {
        self.record("callback_execution_end", json!({"request_id": req_id}));
        self.observed.execution_ends.insert(req_id);
    }

    fn commission_and_fees_report(&mut self, report: &CommissionAndFeesReport) {
        self.record(
            "callback_commission",
            json!({
                "exec_id": report.exec_id,
                "commission_and_fees": report.commission_and_fees.to_string(),
                "currency": report.currency,
                "realized_pnl": report.realized_pnl.to_string(),
                "yield_amount": report.yield_amount.to_string(),
                "yield_redemption_date": report.yield_redemption_date,
            }),
        );
        self.observed.commissions.push(report.clone());
    }

    fn option_lifecycle(&mut self, observation: &OptionLifecycleObservation) {
        self.record(
            "callback_option_lifecycle_unsupported",
            json!({
                "raw_value": observation.raw_value,
                "classification": format!("{:?}", observation.classification),
                "account": observation.account,
                "contract_id": observation.contract_id,
                "execution_id": observation.execution_id,
                "order_id": observation.order_id,
                "api_order_id": observation.api_order_id,
            }),
        );
        self.observed.lifecycle.push(observation.clone());
    }

    fn order_status(
        &mut self,
        order_id: i64,
        status: &str,
        filled: f64,
        remaining: f64,
        average_fill_price: f64,
        perm_id: i64,
        parent_id: i64,
        last_fill_price: f64,
        client_id: i64,
        why_held: &str,
        market_cap_price: f64,
    ) {
        self.record(
            "callback_order_status",
            json!({
                "order_id": order_id,
                "status": status,
                "filled": filled.to_string(),
                "remaining": remaining.to_string(),
                "average_fill_price": average_fill_price.to_string(),
                "perm_id": perm_id,
                "parent_id": parent_id,
                "last_fill_price": last_fill_price.to_string(),
                "client_id": client_id,
                "why_held": why_held,
                "market_cap_price": market_cap_price.to_string(),
            }),
        );
        self.observed.order_status.insert(order_id, status.into());
    }

    fn open_order(
        &mut self,
        order_id: i64,
        contract: &Contract,
        order: &Order,
        state: &ApiOrderState,
    ) {
        self.record(
            "callback_open_order",
            json!({
                "order_id": order_id,
                "contract": contract_value(contract),
                "action": order.action,
                "total_quantity": order.total_quantity.to_string(),
                "order_type": order.order_type,
                "limit_price": order.lmt_price.to_string(),
                "time_in_force": order.tif,
                "order_ref": order.order_ref,
                "status": state.status,
            }),
        );
        self.observed.open_orders.push(SeenOpenOrder {
            order_id,
            contract: contract.clone(),
            order_ref: order.order_ref.clone(),
            action: order.action.clone(),
            account: order.account.clone(),
        });
    }

    fn open_order_end(&mut self) {
        self.record("callback_open_order_end", json!({}));
        self.observed.open_order_ends += 1;
    }

    fn position(&mut self, account: &str, contract: &Contract, quantity: f64, average_cost: f64) {
        self.record(
            "callback_position",
            json!({
                "account": account,
                "contract": contract_value(contract),
                "quantity": quantity.to_string(),
                "average_cost": average_cost.to_string(),
            }),
        );
        self.observed
            .positions
            .push((account.into(), contract.clone(), quantity, average_cost));
    }

    fn position_end(&mut self) {
        self.record("callback_position_end", json!({}));
        self.observed.position_ends += 1;
    }

    fn update_account_value(&mut self, key: &str, value: &str, currency: &str, account: &str) {
        self.record(
            "callback_account_value",
            json!({
                "key": key,
                "value": value,
                "currency": currency,
                "account": account,
            }),
        );
    }

    fn update_portfolio(
        &mut self,
        contract: &Contract,
        position: f64,
        market_price: f64,
        market_value: f64,
        average_cost: f64,
        unrealized_pnl: f64,
        realized_pnl: f64,
        account: &str,
    ) {
        self.record(
            "callback_portfolio",
            json!({
                "account": account,
                "contract": contract_value(contract),
                "position": position.to_string(),
                "market_price": market_price.to_string(),
                "market_value": market_value.to_string(),
                "average_cost": average_cost.to_string(),
                "unrealized_pnl": unrealized_pnl.to_string(),
                "realized_pnl": realized_pnl.to_string(),
            }),
        );
    }

    fn update_account_time(&mut self, timestamp: &str) {
        self.record("callback_account_time", json!({"timestamp": timestamp}));
    }

    fn account_download_end(&mut self, account: &str) {
        self.record("callback_account_download_end", json!({"account": account}));
        self.observed.account_end_accounts.push(account.to_string());
    }

    fn connection_closed(&mut self) {
        self.record("callback_connection_closed", json!({}));
        self.observed.connection_closed = true;
    }
}

struct Session {
    client: EClient,
    control_tx: Sender<ControlCommand>,
    shared: Arc<SharedState>,
    execution_history: String,
    account_request: String,
}

impl Session {
    fn current(&self) -> bool {
        self.client.is_connected()
            && self.shared.orders.execution_history_completion().as_deref()
                == Some(self.execution_history.as_str())
            && self
                .shared
                .portfolio
                .account_image_matches(&self.account_request)
    }

    fn shutdown(&self) {
        self.client.disconnect();
        wire_capture().disarm();
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        wire_capture().disarm();
    }
}

fn pump(session: &Session, probe: &mut Probe) -> Check<()> {
    session.client.process_msgs(probe);
    probe.receipt.check()?;
    probe.reject_lifecycle()?;
    if !session.client.is_connected() || probe.observed.connection_closed {
        return Err("physical broker session closed".into());
    }
    Ok(())
}

fn wait_for(
    session: &Session,
    probe: &mut Probe,
    timeout: Duration,
    mut ready: impl FnMut(&Session, &Probe) -> bool,
) -> Check<()> {
    let deadline = Instant::now() + timeout;
    loop {
        pump(session, probe)?;
        if ready(session, probe) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("bounded callback wait expired".into());
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn api_quantity_fixed(value: f64) -> Check<i64> {
    let scaled = value * QTY_SCALE as f64;
    if !scaled.is_finite()
        || scaled < i64::MIN as f64
        || scaled >= i64::MAX as f64
        || (scaled - scaled.round()).abs() > 1e-6
    {
        return Err("callback quantity cannot be represented exactly in QTY_SCALE".into());
    }
    Ok(scaled.round() as i64)
}

fn callback_position_fixed(
    rows: &[(String, Contract, f64, f64)],
    start: usize,
    account: &str,
    con_id: i64,
) -> Check<i64> {
    let rows = rows[start..]
        .iter()
        .filter(|(row_account, contract, _, _)| row_account == account && contract.con_id == con_id)
        .collect::<Vec<_>>();
    match rows.last() {
        Some((_, _, quantity, _)) => api_quantity_fixed(*quantity),
        None => Ok(0),
    }
}

fn connect_fresh(
    settings: &ConnectionSettings,
    plan: &Plan,
    receipt: Arc<Receipt>,
    stage: &str,
) -> Check<(Session, Probe, FreshSnapshot)> {
    let capture = wire_capture();
    capture.disarm();
    let credentials = Credentials::read(&settings.credential_path)?;
    let config = credentials.gateway_config(&settings.host);
    let (gateway, farm, ccp, hmds) = Gateway::connect(&config).map_err(|_| {
        "paper Gateway connection failed; see private operator diagnostics".to_string()
    })?;
    if gateway.account_id != settings.expected_account || !gateway.account_id.starts_with("DU") {
        return Err(
            "authenticated account did not exactly match the expected paper account".into(),
        );
    }

    let account_id = gateway.account_id.clone();
    let shared = Arc::new(SharedState::new());
    gateway.populate_init_data(&shared);
    let (mut hot_loop, control_tx) =
        gateway.into_hot_loop_with_farms(shared.clone(), None, farm, ccp, hmds, None);
    hot_loop.update_reconnect_auth(
        settings.host.clone(),
        credentials.username.to_string(),
        Zeroizing::new(credentials.password.to_string()),
        true,
    );
    let native_session = capture.arm(
        receipt.clone(),
        &settings.expected_account,
        plan.option_con_id,
        plan.underlying_con_id,
        stage,
    );
    let handle = std::thread::Builder::new()
        .name("option-accounting-hotloop".into())
        .stack_size(64 * 1024 * 1024)
        .spawn(move || hot_loop.run_with_panic_recovery())
        .map_err(|_| {
            capture.disarm();
            "cannot start broker engine thread".to_string()
        })?;
    let client = EClient::from_parts(shared.clone(), control_tx.clone(), handle, account_id);
    let mut session = Session {
        client,
        control_tx,
        shared,
        execution_history: String::new(),
        account_request: String::new(),
    };
    let mut probe = Probe::new(receipt.clone());

    let deadline = Instant::now() + CALLBACK_TIMEOUT;
    loop {
        pump(&session, &mut probe)?;
        let history = session.shared.orders.execution_history_completion();
        let account_rows = session.shared.portfolio.account_rows();
        if let Some(request) = account_rows.request.as_deref() {
            capture.set_account_request(request);
        }
        if let (Some(history), Some(request)) = (history, account_rows.request)
            && account_rows.image_complete
            && session.shared.portfolio.account_download_complete()
            && capture.has_connection_image(native_session, &settings.expected_account, &request)
        {
            session.execution_history = history;
            session.account_request = request;
            break;
        }
        if Instant::now() >= deadline {
            return Err(
                "physical connection did not produce a complete native initial account image"
                    .into(),
            );
        }
        std::thread::sleep(Duration::from_millis(25));
    }

    let position_start = probe.observed.positions.len();
    let position_end_start = probe.observed.position_ends;
    let account_end_start = probe.observed.account_end_accounts.len();
    session.client.req_positions(&mut probe);
    session
        .client
        .req_account_updates(true, &settings.expected_account);
    wait_for(&session, &mut probe, CALLBACK_TIMEOUT, |_, probe| {
        probe.observed.position_ends > position_end_start
            && probe.observed.account_end_accounts[account_end_start..]
                .iter()
                .any(|account| account == &settings.expected_account)
    })?;
    if !session.current() {
        return Err("connection changed while the initial image was delivered".into());
    }

    let option = session
        .shared
        .portfolio
        .position_info(plan.option_con_id)
        .unwrap_or_default();
    let underlying = session
        .shared
        .portfolio
        .position_info(plan.underlying_con_id)
        .unwrap_or_default();
    let callback_option = callback_position_fixed(
        &probe.observed.positions,
        position_start,
        &settings.expected_account,
        plan.option_con_id,
    )?;
    let callback_underlying = callback_position_fixed(
        &probe.observed.positions,
        position_start,
        &settings.expected_account,
        plan.underlying_con_id,
    )?;
    if callback_option != option.position_fixed || callback_underlying != underlying.position_fixed
    {
        return Err("SDK position callbacks disagree with the completed production image".into());
    }

    let account_rows = session.shared.portfolio.account_rows();
    receipt.append(
        "fresh_connection_image",
        stage,
        json!({
            "native_session": native_session,
            "execution_history": session.execution_history,
            "account_request": session.account_request,
            "option_position_fixed_qty_scale": option.position_fixed.to_string(),
            "option_average_cost_raw_price_scale": option.avg_cost.to_string(),
            "underlying_position_fixed_qty_scale": underlying.position_fixed.to_string(),
            "underlying_average_cost_raw_price_scale": underlying.avg_cost.to_string(),
            "callback_option_position_fixed_qty_scale": callback_option.to_string(),
            "callback_underlying_position_fixed_qty_scale": callback_underlying.to_string(),
            "account_rows_time_unix_seconds": account_rows.time_secs,
            "account_rows": account_rows.rows.iter().map(|row| json!({
                "key": row.key,
                "value": row.value,
                "currency": row.currency,
                "ledger": row.ledger,
            })).collect::<Vec<_>>(),
            "freshness": "new physical connection initial native image",
            "cash_caveat": "observed account rows are timestamped broker reports, not proof of trade settlement",
        }),
    );
    receipt.check()?;
    let snapshot = FreshSnapshot {
        native_session,
        execution_history: session.execution_history.clone(),
        account_request: session.account_request.clone(),
        option_position_fixed: option.position_fixed,
        option_average_cost: option.avg_cost,
        underlying_position_fixed: underlying.position_fixed,
        underlying_average_cost: underlying.avg_cost,
    };
    Ok((session, probe, snapshot))
}

fn details_for(
    session: &Session,
    probe: &mut Probe,
    contract: &Contract,
) -> Check<ContractDetails> {
    let req_id = next_request_id();
    let row_start = probe.observed.details.len();
    let error_start = probe.observed.errors.len();
    session
        .client
        .req_contract_details(req_id, contract)
        .map_err(|_| "contract-details request could not be queued".to_string())?;
    wait_for(session, probe, CALLBACK_TIMEOUT, |_, probe| {
        probe.observed.detail_ends.contains(&req_id)
            || probe.observed.errors[error_start..]
                .iter()
                .any(|(id, _, _)| *id == req_id)
    })?;
    if let Some((_, code, _)) = probe.observed.errors[error_start..]
        .iter()
        .find(|(id, _, _)| *id == req_id)
    {
        return Err(format!("contract-details request failed with code {code}"));
    }
    let rows = probe.observed.details[row_start..]
        .iter()
        .filter(|(id, _)| *id == req_id)
        .map(|(_, details)| details.clone())
        .collect::<Vec<_>>();
    if rows.len() != 1 || rows[0].contract.con_id != contract.con_id {
        return Err("contract-details request did not resolve one exact conId".into());
    }
    Ok(rows.into_iter().next().unwrap())
}

fn contains_token(list: &str, expected: &str) -> bool {
    list.split([',', ';'])
        .map(str::trim)
        .any(|value| value.eq_ignore_ascii_case(expected))
}

#[derive(Clone, Copy)]
enum QualificationPurpose {
    NewLongCallEntry,
    ExistingLongRecovery,
}

fn right_is_supported(right: &str, purpose: QualificationPurpose) -> bool {
    matches!(
        (right.to_ascii_uppercase().as_str(), purpose),
        ("C" | "CALL", _) | ("P" | "PUT", QualificationPurpose::ExistingLongRecovery)
    )
}

fn qualify_and_register(
    session: &Session,
    probe: &mut Probe,
    plan: &Plan,
    receipt: &Receipt,
    minimum_days_to_expiry: i64,
    purpose: QualificationPurpose,
) -> Check<(Contract, InstrumentId)> {
    wire_capture().set_stage("qualification");
    let option_query = Contract {
        con_id: plan.option_con_id,
        symbol: plan.symbol.clone(),
        sec_type: "OPT".into(),
        exchange: plan.exchange.clone(),
        currency: "USD".into(),
        ..Default::default()
    };
    let underlying_query = Contract {
        con_id: plan.underlying_con_id,
        symbol: plan.symbol.clone(),
        sec_type: "STK".into(),
        exchange: plan.exchange.clone(),
        currency: "USD".into(),
        ..Default::default()
    };
    let option = details_for(session, probe, &option_query)?;
    let underlying = details_for(session, probe, &underlying_query)?;
    let contract = &option.contract;
    let multiplier = contract.multiplier.parse::<f64>().ok();
    let expiry_text = if option.real_expiration_date.is_empty() {
        &contract.last_trade_date_or_contract_month
    } else {
        &option.real_expiration_date
    };
    let expiry = expiry_text
        .get(..8)
        .and_then(|value| jiff::civil::Date::strptime("%Y%m%d", value).ok())
        .ok_or_else(|| "qualified option has no exact native expiration date".to_string())?;
    let minimum_expiry = jiff::Zoned::now()
        .with_time_zone(jiff::tz::TimeZone::UTC)
        .date()
        .checked_add(jiff::Span::new().days(minimum_days_to_expiry))
        .map_err(|_| "cannot compute minimum safe expiry".to_string())?;
    let option_ok = contract.con_id == plan.option_con_id
        && contract.symbol.eq_ignore_ascii_case(&plan.symbol)
        && contract.sec_type.eq_ignore_ascii_case("OPT")
        && contract.currency.eq_ignore_ascii_case("USD")
        && contract.strike.is_finite()
        && contract.strike > 0.0
        && right_is_supported(&contract.right, purpose)
        && multiplier == Some(OPTION_PREMIUM_MULTIPLIER)
        && option.under_con_id as i64 == plan.underlying_con_id
        && option.under_symbol.eq_ignore_ascii_case(&plan.symbol)
        && option.under_sec_type.eq_ignore_ascii_case("STK")
        && matches!(option.exercise_style, 1 | 2)
        && option.settlement_method == Some(SettlementMethod::PhysicalDelivery)
        && option.basket_composition.is_none()
        && contains_token(&option.order_types, "LMT")
        && contains_token(&option.valid_exchanges, &plan.exchange)
        && expiry >= minimum_expiry;
    let underlying_ok = underlying.contract.con_id == plan.underlying_con_id
        && underlying
            .contract
            .symbol
            .eq_ignore_ascii_case(&plan.symbol)
        && underlying.contract.sec_type.eq_ignore_ascii_case("STK")
        && underlying.contract.currency.eq_ignore_ascii_case("USD")
        && !underlying.contract.exchange.is_empty()
        && underlying.basket_composition.is_none()
        && !underlying.contract.exchange.eq_ignore_ascii_case("BASKET")
        && !underlying
            .contract
            .primary_exchange
            .eq_ignore_ascii_case("BASKET");
    if !option_ok || !underlying_ok {
        return Err("native contract details did not meet the explicit single equity-option probe constraints".into());
    }
    receipt.append(
        "qualified_contract",
        "qualification",
        json!({
            "option": details_value(&option),
            "underlying": details_value(&underlying),
            "minimum_expiration_date": minimum_expiry.strftime("%Y%m%d").to_string(),
            "minimum_days_to_expiry": minimum_days_to_expiry,
            "native_multiplier": contract.multiplier,
            "settlement": "PhysicalDelivery",
            "supported_shape": match purpose {
                QualificationPurpose::NewLongCallEntry => "one new long standard-share call",
                QualificationPurpose::ExistingLongRecovery => "one existing receipt-owned long standard-share call or put",
            },
            "delivery_limitation": "positive native branch: physical settlement, multiplier 100, and direct STK with a nonempty native exchange and no basket markers; these fields still do not establish authoritative deliverable transformation units",
        }),
    );
    receipt.check()?;

    let (reply_tx, reply_rx) = RegistrationReply::channel();
    session
        .control_tx
        .send(ControlCommand::RegisterOptionInstrument {
            contract: Box::new(contract.clone()),
            reply_tx: Some(reply_tx),
        })
        .map_err(|_| "option registration could not be queued".to_string())?;
    let instrument = reply_rx
        .recv_timeout(REGISTRATION_TIMEOUT)
        .map_err(|error| format!("option registration reply failed: {error}"))??;
    receipt.append(
        "option_registration",
        "qualification",
        json!({"instrument": instrument, "admission": "accepted"}),
    );
    receipt.check()?;
    Ok((contract.clone(), instrument))
}

fn capture_quote(
    session: &Session,
    probe: &mut Probe,
    contract: &Contract,
    receipt: &Receipt,
) -> Check<()> {
    wire_capture().set_stage("quote_observation");
    let req_id = next_request_id();
    let error_start = probe.observed.errors.len();
    let queued = session
        .client
        .req_mkt_data(req_id, contract, "", true, false)
        .is_ok();
    let mut authority_error = None;
    if queued {
        let deadline = Instant::now() + Duration::from_secs(12);
        while Instant::now() < deadline
            && !probe.observed.snapshot_ends.contains(&req_id)
            && !probe.observed.errors[error_start..]
                .iter()
                .any(|(id, _, _)| *id == req_id)
        {
            if let Err(error) = pump(session, probe) {
                authority_error = Some(error);
                break;
            }
            if !session.current() {
                authority_error =
                    Some("connection changed during the optional quote observation".to_string());
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        let _ = session.client.cancel_mkt_data(req_id);
    }
    let bid =
        probe.observed.bid.filter(|(id, _, _)| *id == req_id).map(
            |(_, tick_type, value)| json!({"tick_type": tick_type, "value": value.to_string()}),
        );
    let ask =
        probe.observed.ask.filter(|(id, _, _)| *id == req_id).map(
            |(_, tick_type, value)| json!({"tick_type": tick_type, "value": value.to_string()}),
        );
    receipt.append(
        "quote_observation",
        "quote_observation",
        json!({
            "request_id": req_id,
            "queued": queued,
            "bid": bid,
            "ask": ask,
            "snapshot_end": probe.observed.snapshot_ends.contains(&req_id),
            "provenance": "optional SDK snapshot callback",
            "freshness_claimed": false,
            "used_to_derive_order_limit": false,
        }),
    );
    receipt.check()?;
    if let Some(error) = authority_error {
        return Err(error);
    }
    Ok(())
}

fn owned_limit_request(
    order_id: i64,
    instrument: InstrumentId,
    side: Side,
    price: i64,
    order_ref: String,
) -> OrderRequest {
    OrderRequest::SubmitEx {
        order_id,
        instrument,
        side,
        qty: 1,
        kind: OrderKind::Limit { price },
        tif: b'0',
        attrs: OrderAttrs {
            order_ref,
            outside_rth: false,
            ..Default::default()
        },
    }
}

fn guarded_order_write(
    session: &Session,
    receipt: &Receipt,
    request: OrderRequest,
    role: &str,
    order_id: i64,
) -> Check<()> {
    let shared = session.shared.clone();
    let account_request = session.account_request.clone();
    let write_receipt = session.client.send_order_guarded(
        request,
        session.execution_history.clone(),
        move || {
            if shared.portfolio.account_image_matches(&account_request) {
                Ok(())
            } else {
                Err("account image changed before first order write".into())
            }
        },
    )?;
    let outcome = write_receipt
        .wait_timeout(REGISTRATION_TIMEOUT)
        .unwrap_or_else(|| write_receipt.cancel());
    receipt.append(
        "order_transport",
        role,
        json!({
            "order_id": order_id,
            "outcome": format!("{outcome:?}"),
            "meaning": "transport write completion only; not broker acceptance",
        }),
    );
    receipt.check()?;
    if outcome != OrderWriteOutcome::Written {
        return Err(format!(
            "order transport did not prove a complete write: {outcome:?}"
        ));
    }
    Ok(())
}

struct OwnedLimitSpec {
    side: Side,
    price: i64,
    order_ref: String,
}

fn submit_owned_limit(
    session: &Session,
    receipt: &Receipt,
    plan: &mut Plan,
    instrument: InstrumentId,
    role: &str,
    spec: OwnedLimitSpec,
) -> Check<i64> {
    let OwnedLimitSpec {
        side,
        price,
        order_ref,
    } = spec;
    if !session.current() {
        return Err("connection is no longer the reconciled physical session".into());
    }
    let order_id = session.client.next_order_id();
    plan.owned_order_refs.insert(order_ref.clone());
    receipt.append(
        "order_intent",
        role,
        json!({
            "role": role,
            "order_id": order_id,
            "order_ref": order_ref,
            "account": plan.expected_account,
            "option_con_id": plan.option_con_id,
            "side": match side { Side::Buy => "BUY", Side::Sell => "SELL", Side::ShortSell => "SHORT_SELL" },
            "quantity_request": "one whole native order unit",
            "economic_unit_interpretation": null,
            "limit_raw_price_scale": price.to_string(),
            "time_in_force": "DAY",
        }),
    );
    receipt.check()?;
    guarded_order_write(
        session,
        receipt,
        owned_limit_request(order_id, instrument, side, price, order_ref),
        role,
        order_id,
    )?;
    Ok(order_id)
}

#[derive(Debug)]
struct FillEvidence {
    execution_ids: BTreeSet<String>,
    quoted_value: f64,
    derived_premium_cash: f64,
}

fn execution_family(exec_id: &str) -> (&str, u64) {
    match exec_id.rsplit_once('.') {
        Some((family, revision)) => u64::from_str_radix(revision, 16)
            .map(|revision| (family, revision))
            .unwrap_or((exec_id, 0)),
        None => (exec_id, 0),
    }
}

fn fill_price_at_or_better(side: &str, price: f64, limit: i64) -> bool {
    if !price.is_finite() || price <= 0.0 {
        return false;
    }
    let limit = limit as f64 / PRICE_SCALE as f64;
    match side.to_ascii_uppercase().as_str() {
        "BOT" | "BUY" => price <= limit,
        "SLD" | "SELL" => price >= limit,
        _ => false,
    }
}

fn derived_premium_cash(side: &str, quoted_value: f64) -> Check<f64> {
    if !quoted_value.is_finite() || quoted_value <= 0.0 {
        return Err("owned execution quoted value must be positive and finite".into());
    }
    match side.to_ascii_uppercase().as_str() {
        "BOT" | "BUY" => Ok(-quoted_value * OPTION_PREMIUM_MULTIPLIER),
        "SLD" | "SELL" => Ok(quoted_value * OPTION_PREMIUM_MULTIPLIER),
        _ => Err("owned execution has an unsupported side".into()),
    }
}

fn owned_fill(
    rows: &[SeenExecution],
    start: usize,
    order_id: i64,
    order_ref: &str,
    side: &str,
    limit: i64,
    plan: &Plan,
) -> Check<Option<FillEvidence>> {
    let mut executions = BTreeMap::<String, &Execution>::new();
    let mut families = BTreeSet::new();
    for row in rows[start..]
        .iter()
        .filter(|row| row.execution.order_id == order_id)
    {
        if row.execution.order_ref != order_ref
            || row.execution.acct_number != plan.expected_account
            || row.contract.con_id != plan.option_con_id
            || !row.execution.side.eq_ignore_ascii_case(side)
            || row.execution.exec_id.is_empty()
            || row.execution.pending_price_revision
        {
            return Err(
                "order execution identity was incomplete or contradicted the durable intent".into(),
            );
        }
        if let Some(previous) = executions.get(&row.execution.exec_id) {
            if previous.shares != row.execution.shares
                || previous.price != row.execution.price
                || previous.side != row.execution.side
            {
                return Err("duplicate execution id carried conflicting economics".into());
            }
            continue;
        }
        let (family, revision) = execution_family(&row.execution.exec_id);
        if revision > 1 || !families.insert(family.to_string()) {
            return Err(
                "execution replacement/correction is unsupported by this accounting probe".into(),
            );
        }
        if !fill_price_at_or_better(side, row.execution.price, limit) {
            return Err(
                "owned execution price was not positive and at or better than its limit".into(),
            );
        }
        executions.insert(row.execution.exec_id.clone(), &row.execution);
    }
    let mut total = 0i64;
    let mut quoted_value = 0.0;
    for execution in executions.values() {
        let fixed = api_quantity_fixed(execution.shares)?;
        if fixed <= 0 {
            return Err("owned execution has a non-positive quantity".into());
        }
        total = total
            .checked_add(fixed)
            .ok_or_else(|| "owned execution quantity overflow".to_string())?;
        quoted_value += execution.shares * execution.price;
        if !quoted_value.is_finite() {
            return Err("owned execution quoted value is not finite".into());
        }
    }
    if total == 0 {
        return Ok(None);
    }
    if total != QTY_SCALE {
        return Err("owned order did not fill exactly one native whole unit".into());
    }
    let derived_cash = derived_premium_cash(side, quoted_value)?;
    Ok(Some(FillEvidence {
        execution_ids: executions.into_keys().collect(),
        quoted_value,
        derived_premium_cash: derived_cash,
    }))
}

struct OwnedFillExpectation<'a> {
    order_id: i64,
    order_ref: &'a str,
    side: &'a str,
    limit: i64,
    execution_start: usize,
}

fn wait_owned_fill(
    session: &Session,
    probe: &mut Probe,
    plan: &Plan,
    expectation: OwnedFillExpectation<'_>,
) -> Check<FillEvidence> {
    let OwnedFillExpectation {
        order_id,
        order_ref,
        side,
        limit,
        execution_start,
    } = expectation;
    let deadline = Instant::now() + ORDER_TIMEOUT;
    loop {
        pump(session, probe)?;
        if let Some(fill) = owned_fill(
            &probe.observed.executions,
            execution_start,
            order_id,
            order_ref,
            side,
            limit,
            plan,
        )? && probe
            .observed
            .order_status
            .get(&order_id)
            .map(String::as_str)
            == Some("Filled")
        {
            return Ok(fill);
        }
        if matches!(
            probe
                .observed
                .order_status
                .get(&order_id)
                .map(String::as_str),
            Some("Cancelled" | "Inactive" | "ApiCancelled")
        ) {
            return Err("owned order became terminal without the exact fill".into());
        }
        if Instant::now() >= deadline {
            return Err("owned order did not fill inside the bounded wait".into());
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn cancel_owned_guarded(
    session: &Session,
    receipt: &Receipt,
    order_id: i64,
    role: &str,
) -> Check<()> {
    if !session.current() {
        return Err("connection changed before the exact owned cancellation".into());
    }
    receipt.append(
        "cancel_intent",
        role,
        json!({"order_id": order_id, "ownership": "exact receipt order_ref"}),
    );
    receipt.check()?;
    guarded_order_write(
        session,
        receipt,
        OrderRequest::Cancel { order_id },
        role,
        order_id,
    )
}

fn cancel_owned_best_effort(session: &Session, receipt: &Receipt, order_id: i64, role: &str) {
    if let Err(error) = cancel_owned_guarded(session, receipt, order_id, role) {
        receipt.append(
            "cancel_incomplete",
            role,
            json!({"order_id": order_id, "error": error}),
        );
    }
}

fn sdk_derived_fill_cash(session: &Session, con_id: i64) -> Check<f64> {
    match session
        .shared
        .portfolio
        .money_since_seed(&session.shared.reference)
        .get(&con_id)
        .copied()
    {
        None => Ok(0.0),
        Some(Some(value)) if value.is_finite() => Ok(value),
        Some(Some(_)) => Err("SDK derived fill cash is not finite".into()),
        Some(None) => Err("SDK cannot establish option premium cash units".into()),
    }
}

fn cash_values_correspond(actual: f64, expected: f64) -> bool {
    actual.is_finite()
        && expected.is_finite()
        && (actual - expected).abs() <= 1e-8_f64.max(expected.abs() * 1e-12)
}

struct PostFillExpectation<'a> {
    marker: (u64, u64, usize, usize, usize),
    callback_position_start: usize,
    fill: &'a FillEvidence,
    order_ref: &'a str,
    derived_cash_before: Check<f64>,
    expected_whole_quantity: i64,
    stage: &'a str,
}

fn wait_post_fill_evidence(
    session: &Session,
    probe: &mut Probe,
    plan: &Plan,
    receipt: &Receipt,
    expectation: PostFillExpectation<'_>,
) -> Check<()> {
    let PostFillExpectation {
        marker,
        callback_position_start,
        fill,
        order_ref,
        derived_cash_before,
        expected_whole_quantity,
        stage,
    } = expectation;
    let derived_cash_before = derived_cash_before?;
    let expected_fixed = expected_whole_quantity * QTY_SCALE;
    let deadline = Instant::now() + CALLBACK_TIMEOUT;
    loop {
        pump(session, probe)?;
        if !session.current() {
            return Err("connection changed before post-fill evidence completed".into());
        }
        let callback = probe.observed.positions[callback_position_start..]
            .iter()
            .rev()
            .find(|(account, contract, _, _)| {
                account == &plan.expected_account && contract.con_id == plan.option_con_id
            })
            .map(|(_, _, quantity, _)| api_quantity_fixed(*quantity))
            .transpose()?;
        let sdk = session
            .shared
            .portfolio
            .position_info(plan.option_con_id)
            .unwrap_or_default();
        let raw_ready = wire_capture().post_fill_evidence(
            marker,
            expected_whole_quantity as f64,
            &fill.execution_ids,
            order_ref,
        );
        if raw_ready && callback == Some(expected_fixed) && sdk.position_fixed == expected_fixed {
            let derived_cash_after = sdk_derived_fill_cash(session, plan.option_con_id)?;
            let derived_cash_delta = derived_cash_after - derived_cash_before;
            if !cash_values_correspond(derived_cash_delta, fill.derived_premium_cash) {
                return Err(format!(
                    "SDK derived fill cash delta {derived_cash_delta} did not equal reported premium cash {}",
                    fill.derived_premium_cash
                ));
            }
            let raw = wire_capture()
                .post_fill_portfolio_row(marker, expected_whole_quantity as f64)
                .ok_or_else(|| "raw portfolio row disappeared".to_string())?;
            receipt.append(
                "post_fill_evidence",
                stage,
                json!({
                    "execution_ids": fill.execution_ids,
                    "reported_fill_quote_value": fill.quoted_value.to_string(),
                    "qualified_premium_multiplier": OPTION_PREMIUM_MULTIPLIER.to_string(),
                    "expected_sdk_derived_fill_cash_delta": fill.derived_premium_cash.to_string(),
                    "sdk_derived_fill_cash_before": derived_cash_before.to_string(),
                    "sdk_derived_fill_cash_after": derived_cash_after.to_string(),
                    "sdk_derived_fill_cash_delta": derived_cash_delta.to_string(),
                    "sdk_position_fixed_qty_scale": sdk.position_fixed.to_string(),
                    "sdk_average_cost_raw_price_scale": sdk.avg_cost.to_string(),
                    "sdk_average_cost_interpretation": "opaque raw broker value; no option unit normalization claimed",
                    "raw_portfolio": {
                        "native_session": raw.session,
                        "native_sequence": raw.sequence,
                        "account_request": raw.account_request,
                        "con_id": raw.con_id,
                        "quantity": raw.quantity,
                        "average_cost": raw.average_cost,
                        "market_price": raw.market_price,
                        "market_value": raw.market_value,
                        "unrealized_pnl": raw.unrealized_pnl,
                        "realized_pnl": raw.realized_pnl,
                    },
                    "account_row_caveat": "a later account row proves report timing only, not settled cash impact",
                    "derived_cash_caveat": "SDK fill quantity times reported premium quote times qualified multiplier; not settled account cash",
                }),
            );
            receipt.check()?;
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(
                "fresh raw execution/account/portfolio evidence did not corroborate the fill"
                    .into(),
            );
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn open_orders_snapshot(session: &Session, probe: &mut Probe) -> Check<Vec<SeenOpenOrder>> {
    let row_start = probe.observed.open_orders.len();
    let end_start = probe.observed.open_order_ends;
    session.client.req_all_open_orders(probe);
    wait_for(session, probe, CALLBACK_TIMEOUT, |_, probe| {
        probe.observed.open_order_ends > end_start
    })?;
    let mut by_id = BTreeMap::new();
    for row in &probe.observed.open_orders[row_start..] {
        by_id.insert(row.order_id, row.clone());
    }
    Ok(by_id.into_values().collect())
}

fn ensure_no_selected_working(session: &Session, probe: &mut Probe, plan: &Plan) -> Check<()> {
    let rows = open_orders_snapshot(session, probe)?;
    if !session.current() {
        return Err("connection changed during the pre-entry open-order snapshot".into());
    }
    if rows
        .iter()
        .any(|row| row.contract.con_id == plan.option_con_id)
    {
        return Err("selected option already has a working order".into());
    }
    Ok(())
}

fn cancel_receipt_owned_working(
    session: &Session,
    probe: &mut Probe,
    plan: &Plan,
    receipt: &Receipt,
) -> Check<bool> {
    wire_capture().set_stage("recovery_open_orders");
    let rows = open_orders_snapshot(session, probe)?;
    if !session.current() {
        return Err("connection changed during the recovery open-order snapshot".into());
    }
    let mut cancel_ids = BTreeSet::new();
    for row in &rows {
        if row.contract.con_id == plan.option_con_id
            && !plan.owned_order_refs.contains(&row.order_ref)
        {
            return Err("an unowned working order exists on the selected option".into());
        }
        if !plan.owned_order_refs.contains(&row.order_ref) {
            continue;
        }
        if row.contract.con_id != plan.option_con_id
            || row.account != plan.expected_account
            || !matches!(row.action.to_ascii_uppercase().as_str(), "BUY" | "SELL")
        {
            return Err("receipt-owned open order identity is contradictory".into());
        }
        cancel_ids.insert(row.order_id);
    }
    for order_id in &cancel_ids {
        cancel_owned_guarded(session, receipt, *order_id, "recovery_open_orders")?;
    }
    if !cancel_ids.is_empty() {
        wait_for(session, probe, CANCEL_TIMEOUT, |_, probe| {
            cancel_ids.iter().all(|order_id| {
                matches!(
                    probe
                        .observed
                        .order_status
                        .get(order_id)
                        .map(String::as_str),
                    Some("Cancelled" | "Filled" | "Inactive" | "ApiCancelled")
                )
            })
        })?;
    }
    let after = open_orders_snapshot(session, probe)?;
    if !session.current() {
        return Err("connection changed while receipt-owned cancellations completed".into());
    }
    if after.iter().any(|row| {
        row.contract.con_id == plan.option_con_id || plan.owned_order_refs.contains(&row.order_ref)
    }) {
        return Err("selected option still has a working order after exact cancellation".into());
    }
    Ok(!cancel_ids.is_empty())
}

fn utc_timestamp(seconds: u64) -> String {
    let days = seconds / 86_400;
    let second_of_day = seconds % 86_400;
    let (year, month, day) = ibx::gateway::days_to_ymd(days);
    format!(
        "{year:04}{month:02}{day:02}-{:02}:{:02}:{:02}",
        second_of_day / 3_600,
        (second_of_day % 3_600) / 60,
        second_of_day % 60
    )
}

fn frozen_history_end_after(completed_through: SystemTime) -> Check<String> {
    let required_second = completed_through
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "history boundary predates Unix epoch".to_string())?
        .as_secs();
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "system time predates Unix epoch".to_string())?
            .as_secs();
        if now > required_second {
            return Ok(utc_timestamp(now - 1));
        }
        if Instant::now() >= deadline {
            return Err("could not freeze history after the completed order/cancel second".into());
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

#[derive(Clone, Debug)]
struct OwnedHistoryRow {
    execution_id: String,
    order_ref: String,
    account: String,
    con_id: i64,
    side: String,
    shares: f64,
    price: f64,
    pending_price_revision: bool,
}

#[derive(Debug, PartialEq, Eq)]
struct OwnedHistoryProjection {
    buys: i64,
    sells: i64,
    net_fixed: i64,
    execution_ids: BTreeSet<String>,
}

fn project_owned_history(
    rows: &[OwnedHistoryRow],
    owned_order_refs: &BTreeSet<String>,
    expected_account: &str,
    option_con_id: i64,
) -> Check<OwnedHistoryProjection> {
    let mut by_id = BTreeMap::<String, &OwnedHistoryRow>::new();
    let mut families = BTreeSet::new();
    for row in rows {
        if !owned_order_refs.contains(&row.order_ref) {
            continue;
        }
        if row.execution_id.is_empty()
            || row.account != expected_account
            || row.con_id != option_con_id
            || row.pending_price_revision
        {
            return Err("owned history row has contradictory identity or revision state".into());
        }
        if let Some(previous) = by_id.get(&row.execution_id) {
            if previous.order_ref != row.order_ref
                || previous.account != row.account
                || previous.con_id != row.con_id
                || previous.side != row.side
                || previous.shares.to_bits() != row.shares.to_bits()
                || previous.price.to_bits() != row.price.to_bits()
                || previous.pending_price_revision != row.pending_price_revision
            {
                return Err("history repeated an execution id with conflicting economics".into());
            }
            continue;
        }
        let (family, revision) = execution_family(&row.execution_id);
        if revision > 1 || !families.insert(family.to_string()) {
            return Err("corrected/replaced execution family is unsupported by this probe".into());
        }
        by_id.insert(row.execution_id.clone(), row);
    }

    let mut buys = 0i64;
    let mut sells = 0i64;
    for row in by_id.values() {
        let quantity = api_quantity_fixed(row.shares)?;
        if quantity <= 0 {
            return Err("owned history row has non-positive quantity".into());
        }
        match row.side.to_ascii_uppercase().as_str() {
            "BOT" | "BUY" => buys = buys.checked_add(quantity).ok_or("buy quantity overflow")?,
            "SLD" | "SELL" => {
                sells = sells
                    .checked_add(quantity)
                    .ok_or("sell quantity overflow")?
            }
            _ => return Err("owned history row has an unsupported side".into()),
        }
    }
    if buys > QTY_SCALE || sells > QTY_SCALE || sells > buys {
        return Err("owned execution net is negative or exceeds the one-unit probe".into());
    }
    let net_fixed = buys - sells;
    if !matches!(net_fixed, 0 | QTY_SCALE) {
        return Err("owned execution net is not exactly flat or one native whole unit".into());
    }
    Ok(OwnedHistoryProjection {
        buys,
        sells,
        net_fixed,
        execution_ids: by_id.into_keys().collect(),
    })
}

fn require_history_execution_ids(
    actual: &BTreeSet<String>,
    required: &BTreeSet<String>,
) -> Check<()> {
    if required.is_subset(actual) {
        Ok(())
    } else {
        Err("fresh history omitted a previously observed owned execution".into())
    }
}

struct HistoryState {
    net_fixed: i64,
    execution_ids: BTreeSet<String>,
    fees_complete: bool,
    fee_error: Option<String>,
}

#[derive(Debug, PartialEq)]
struct VerifiedCommission {
    execution_id: String,
    amount: f64,
    currency: String,
}

fn verify_commission_correspondence(
    execution_ids: &BTreeSet<String>,
    callbacks: &[CommissionAndFeesReport],
    native: &[NativeCommission],
) -> Check<Vec<VerifiedCommission>> {
    let mut verified = Vec::new();
    for execution_id in execution_ids {
        let callback_rows = callbacks
            .iter()
            .filter(|row| row.exec_id == *execution_id)
            .collect::<Vec<_>>();
        let Some(first_callback) = callback_rows.first() else {
            return Err(format!(
                "owned execution {execution_id} has no commission callback"
            ));
        };
        if !first_callback.commission_and_fees.is_finite()
            || first_callback.commission_and_fees == f64::MAX
            || first_callback.currency.is_empty()
        {
            return Err(format!(
                "owned execution {execution_id} has an incomplete commission callback"
            ));
        }
        if callback_rows.iter().skip(1).any(|row| {
            row.commission_and_fees.to_bits() != first_callback.commission_and_fees.to_bits()
                || row.currency != first_callback.currency
        }) {
            return Err(format!(
                "owned execution {execution_id} has conflicting commission callbacks"
            ));
        }

        let native_rows = native
            .iter()
            .filter(|row| row.execution_id == *execution_id)
            .collect::<Vec<_>>();
        if native_rows.is_empty() {
            return Err(format!(
                "owned execution {execution_id} has no native commission frame"
            ));
        }
        let raw_corresponds = native_rows.iter().any(|row| {
            let amount = row
                .amount
                .as_deref()
                .and_then(|value| value.parse::<f64>().ok());
            amount.is_some_and(|amount| {
                amount.is_finite()
                    && amount != f64::MAX
                    && amount.to_bits() == first_callback.commission_and_fees.to_bits()
            }) && row.currency.as_deref() == Some(first_callback.currency.as_str())
        });
        if !raw_corresponds {
            return Err(format!(
                "owned execution {execution_id} has no explicit native amount/currency matching its callback"
            ));
        }
        verified.push(VerifiedCommission {
            execution_id: execution_id.clone(),
            amount: first_callback.commission_and_fees,
            currency: first_callback.currency.clone(),
        });
    }
    Ok(verified)
}

fn query_owned_history(
    session: &Session,
    probe: &mut Probe,
    plan: &Plan,
    receipt: &Receipt,
    stage: &str,
    completed_through: SystemTime,
) -> Check<HistoryState> {
    wire_capture().set_stage(stage);
    let end = frozen_history_end_after(completed_through)?;
    if end < plan.history_start {
        return Err("no frozen completed history interval exists yet".into());
    }
    let req_id = next_request_id();
    let execution_start = probe.observed.executions.len();
    let commission_start = probe.observed.commissions.len();
    let native_commission_marker = wire_capture().commission_marker();
    let error_start = probe.observed.errors.len();
    let filter = ExecutionFilter {
        acct_code: plan.expected_account.clone(),
        symbol: plan.symbol.clone(),
        sec_type: "OPT".into(),
        ..Default::default()
    };
    session
        .client
        .req_executions_range(req_id, &plan.history_start, &end, &filter, probe);
    wait_for(session, probe, CALLBACK_TIMEOUT, |_, probe| {
        probe.observed.execution_ends.contains(&req_id)
            || probe.observed.errors[error_start..]
                .iter()
                .any(|(id, _, _)| *id == req_id)
    })?;
    if let Some((_, code, _)) = probe.observed.errors[error_start..]
        .iter()
        .find(|(id, _, _)| *id == req_id)
    {
        return Err(format!("execution-history query failed with code {code}"));
    }
    if !session.current() {
        return Err("connection changed during the owned execution-history query".into());
    }

    let rows = probe.observed.executions[execution_start..]
        .iter()
        .filter(|row| row.req_id == req_id)
        .map(|row| OwnedHistoryRow {
            execution_id: row.execution.exec_id.clone(),
            order_ref: row.execution.order_ref.clone(),
            account: row.execution.acct_number.clone(),
            con_id: row.contract.con_id,
            side: row.execution.side.clone(),
            shares: row.execution.shares,
            price: row.execution.price,
            pending_price_revision: row.execution.pending_price_revision,
        })
        .collect::<Vec<_>>();
    let projection = project_owned_history(
        &rows,
        &plan.owned_order_refs,
        &plan.expected_account,
        plan.option_con_id,
    )?;
    let OwnedHistoryProjection {
        buys,
        sells,
        net_fixed,
        execution_ids,
    } = projection;
    let commissions = &probe.observed.commissions[commission_start..];
    let native_commissions = wire_capture().commissions_since(native_commission_marker);
    let (verified_commissions, fee_error) =
        match verify_commission_correspondence(&execution_ids, commissions, &native_commissions) {
            Ok(rows) => (rows, None),
            Err(error) => (Vec::new(), Some(error)),
        };
    let fees_complete = fee_error.is_none();
    receipt.append(
        "history_reconciliation",
        stage,
        json!({
            "request_id": req_id,
            "start_utc_inclusive": plan.history_start,
            "end_utc_inclusive_frozen_past": end,
            "account_filter": plan.expected_account,
            "symbol_filter": plan.symbol,
            "security_type_filter": "OPT",
            "local_contract_filter": plan.option_con_id,
            "owned_order_refs": plan.owned_order_refs,
            "owned_execution_ids": execution_ids,
            "buy_fixed_qty_scale": buys.to_string(),
            "sell_fixed_qty_scale": sells.to_string(),
            "net_fixed_qty_scale": net_fixed.to_string(),
            "fees_complete": fees_complete,
            "fee_error": fee_error.as_deref(),
            "fee_evidence": "each callback amount and currency exactly matches a native 6040=60 frame carrying tag 6378 and tag 6381",
            "verified_native_commissions": verified_commissions.iter().map(|row| json!({
                "exec_id": row.execution_id,
                "commission_and_fees": row.amount.to_string(),
                "currency": row.currency,
            })).collect::<Vec<_>>(),
            "commissions": commissions.iter().filter(|report| execution_ids.contains(&report.exec_id)).map(|report| json!({
                "exec_id": report.exec_id,
                "commission_and_fees": report.commission_and_fees.to_string(),
                "currency": report.currency,
                "realized_pnl": report.realized_pnl.to_string(),
            })).collect::<Vec<_>>(),
        }),
    );
    receipt.check()?;
    Ok(HistoryState {
        net_fixed,
        execution_ids,
        fees_complete,
        fee_error,
    })
}

fn connect_after_owned_cancels(
    settings: &ConnectionSettings,
    plan: &Plan,
    receipt: Arc<Receipt>,
    stage: &str,
) -> Check<(Session, Probe, FreshSnapshot)> {
    let (session, mut probe, snapshot) = connect_fresh(settings, plan, receipt.clone(), stage)?;
    let cancelled = cancel_receipt_owned_working(&session, &mut probe, plan, &receipt)?;
    if !cancelled {
        return Ok((session, probe, snapshot));
    }

    session.shutdown();
    drop(session);
    drop(probe);
    let reconnect_stage = format!("{stage}_after_cancel");
    let (fresh_session, mut fresh_probe, fresh_snapshot) =
        connect_fresh(settings, plan, receipt.clone(), &reconnect_stage)?;
    if cancel_receipt_owned_working(&fresh_session, &mut fresh_probe, plan, &receipt)? {
        fresh_session.shutdown();
        return Err(
            "another receipt-owned order remained after the cancellation reconnect; rerun recovery"
                .into(),
        );
    }
    Ok((fresh_session, fresh_probe, fresh_snapshot))
}

fn reconcile_and_close(
    settings: &ConnectionSettings,
    receipt: Arc<Receipt>,
    plan: &mut Plan,
    required_entry_execution_ids: Option<&BTreeSet<String>>,
) -> Check<()> {
    if plan.baseline_option_fixed != Some(0) || plan.baseline_underlying_fixed.is_none() {
        return Err("receipt lacks the completed flat option/underlying baseline".into());
    }

    let (session, mut probe, snapshot) =
        connect_after_owned_cancels(settings, plan, receipt.clone(), "recovery_reconnect")?;
    let mut required_before_close = plan.known_execution_ids.clone();
    if let Some(required) = required_entry_execution_ids {
        required_before_close.extend(required.iter().cloned());
    }
    let first = (|| -> Check<(Option<String>, BTreeSet<String>)> {
        let history = query_owned_history(
            &session,
            &mut probe,
            plan,
            &receipt,
            "recovery_history_before_close",
            SystemTime::now(),
        )?;
        require_history_execution_ids(&history.execution_ids, &required_before_close)?;
        if snapshot.option_position_fixed != history.net_fixed {
            return Err("fresh broker position does not equal receipt-owned execution net".into());
        }
        let mut required_final_execution_ids = history.execution_ids.clone();
        if history.net_fixed == 0 {
            return Ok((None, required_final_execution_ids));
        }

        let (_, instrument) = qualify_and_register(
            &session,
            &mut probe,
            plan,
            &receipt,
            0,
            QualificationPurpose::ExistingLongRecovery,
        )?;
        wire_capture().set_stage("recovery_close");
        let derived_cash_before = sdk_derived_fill_cash(&session, plan.option_con_id);
        let marker = wire_capture().marker();
        let execution_start = probe.observed.executions.len();
        let callback_position_start = probe.observed.positions.len();
        let order_ref = new_order_ref("recovery-close");
        let close_limit = plan.close_limit;
        let order_id = match submit_owned_limit(
            &session,
            &receipt,
            plan,
            instrument,
            "recovery_close",
            OwnedLimitSpec {
                side: Side::Sell,
                price: close_limit,
                order_ref: order_ref.clone(),
            },
        ) {
            Ok(order_id) => order_id,
            Err(error) => return Ok((Some(error), required_final_execution_ids)),
        };
        let fill = match wait_owned_fill(
            &session,
            &mut probe,
            plan,
            OwnedFillExpectation {
                order_id,
                order_ref: &order_ref,
                side: "SLD",
                limit: close_limit,
                execution_start,
            },
        ) {
            Ok(fill) => fill,
            Err(error) => {
                cancel_owned_best_effort(&session, &receipt, order_id, "recovery_close");
                return Ok((Some(error), required_final_execution_ids));
            }
        };
        required_final_execution_ids.extend(fill.execution_ids.iter().cloned());
        let evidence_gap = wait_post_fill_evidence(
            &session,
            &mut probe,
            plan,
            &receipt,
            PostFillExpectation {
                marker,
                callback_position_start,
                fill: &fill,
                order_ref: &order_ref,
                derived_cash_before,
                expected_whole_quantity: 0,
                stage: "recovery_close",
            },
        )
        .err();
        Ok((evidence_gap, required_final_execution_ids))
    })();
    session.shutdown();
    let (close_evidence_gap, required_final_execution_ids) = first?;

    let (final_session, mut final_probe, final_snapshot) =
        connect_after_owned_cancels(settings, plan, receipt.clone(), "final_reconnect")?;
    let final_result = (|| -> Check<()> {
        let history = query_owned_history(
            &final_session,
            &mut final_probe,
            plan,
            &receipt,
            "final_history",
            SystemTime::now(),
        )?;
        require_history_execution_ids(&history.execution_ids, &required_final_execution_ids)?;
        if history.net_fixed != 0 || final_snapshot.option_position_fixed != 0 {
            return Err(
                "final physical reconnect is not flat; rerun recovery with a reviewed limit".into(),
            );
        }
        if final_snapshot.underlying_position_fixed != plan.baseline_underlying_fixed.unwrap() {
            return Err("underlying position changed during the option probe; lifecycle accounting is unsupported".into());
        }
        if !history.fees_complete {
            return Err(format!(
                "final owned execution history did not prove native fees for every execution: {}",
                history
                    .fee_error
                    .as_deref()
                    .unwrap_or("unknown fee evidence gap")
            ));
        }
        if let Some(gap) = close_evidence_gap.as_deref() {
            return Err(format!(
                "position was safely reconciled flat, but close evidence was incomplete: {gap}"
            ));
        }
        let verification_kind = if history.execution_ids.is_empty() {
            "recovery_no_entry_flat_verified"
        } else {
            "recovery_flat_verified"
        };
        receipt.append(
            verification_kind,
            "final_reconnect",
            json!({
                "flat_option_position_fixed_qty_scale": final_snapshot.option_position_fixed.to_string(),
                "underlying_position_fixed_qty_scale": final_snapshot.underlying_position_fixed.to_string(),
                "owned_execution_ids": history.execution_ids,
                "fees_complete": history.fees_complete,
                "economic_unit_interpretation": null,
                "lifecycle_transformations_supported": false,
            }),
        );
        receipt.check()
    })();
    final_session.shutdown();
    final_result
}

fn entry_acceptance_run() -> Check<()> {
    let settings = ConnectionSettings::from_env()?;
    let receipt_path = PathBuf::from(env_required("IB_OPTION_ACCOUNTING_RECEIPT")?);
    let mut plan = Plan::from_entry_env(&settings)?;
    let receipt = Receipt::create(&receipt_path)?;
    plan.record_header(&receipt);
    receipt.check()?;

    let (session, mut probe, baseline) =
        connect_fresh(&settings, &plan, receipt.clone(), "entry_baseline")?;
    if let Err(error) = plan.record_baseline(&baseline, &receipt) {
        session.shutdown();
        return Err(error);
    }
    let (contract, instrument) = match qualify_and_register(
        &session,
        &mut probe,
        &plan,
        &receipt,
        7,
        QualificationPurpose::NewLongCallEntry,
    ) {
        Ok(value) => value,
        Err(error) => {
            session.shutdown();
            return Err(error);
        }
    };
    if let Err(error) = capture_quote(&session, &mut probe, &contract, &receipt)
        .and_then(|()| ensure_no_selected_working(&session, &mut probe, &plan))
    {
        session.shutdown();
        return Err(error);
    }

    wire_capture().set_stage("entry");
    let derived_cash_before = match sdk_derived_fill_cash(&session, plan.option_con_id) {
        Ok(value) => value,
        Err(error) => {
            session.shutdown();
            return Err(error);
        }
    };
    let marker = wire_capture().marker();
    let execution_start = probe.observed.executions.len();
    let callback_position_start = probe.observed.positions.len();
    let entry_ref = plan.entry_order_ref.clone();
    let entry_limit = plan.entry_limit;
    let entry_order = submit_owned_limit(
        &session,
        &receipt,
        &mut plan,
        instrument,
        "entry",
        OwnedLimitSpec {
            side: Side::Buy,
            price: entry_limit,
            order_ref: entry_ref.clone(),
        },
    );
    let entry_result = match entry_order {
        Ok(order_id) => {
            let result = wait_owned_fill(
                &session,
                &mut probe,
                &plan,
                OwnedFillExpectation {
                    order_id,
                    order_ref: &entry_ref,
                    side: "BOT",
                    limit: entry_limit,
                    execution_start,
                },
            )
            .and_then(|fill| {
                wait_post_fill_evidence(
                    &session,
                    &mut probe,
                    &plan,
                    &receipt,
                    PostFillExpectation {
                        marker,
                        callback_position_start,
                        fill: &fill,
                        order_ref: &entry_ref,
                        derived_cash_before: Ok(derived_cash_before),
                        expected_whole_quantity: 1,
                        stage: "entry",
                    },
                )?;
                Ok(fill)
            });
            if result.is_err() {
                cancel_owned_best_effort(&session, &receipt, order_id, "entry");
            }
            result
        }
        Err(error) => Err(error),
    };
    session.shutdown();

    let required_entry_execution_ids = entry_result.as_ref().ok().map(|fill| &fill.execution_ids);
    let recovery_result = reconcile_and_close(
        &settings,
        receipt.clone(),
        &mut plan,
        required_entry_execution_ids,
    );
    match (entry_result, recovery_result) {
        (Ok(fill), Ok(())) => {
            receipt.append(
                "acceptance",
                "entry_acceptance_complete",
                json!({
                    "entry_evidence_complete": true,
                    "receipt_owned_recovery_flat_verified": true,
                    "entry_execution_ids": fill.execution_ids,
                    "economic_unit_interpretation": null,
                    "lifecycle_transformations_supported": false,
                }),
            );
            receipt.check()
        }
        (Err(entry), Ok(())) => Err(format!(
            "entry evidence was incomplete, but receipt-owned recovery reached flat: {entry}"
        )),
        (Ok(_), Err(recovery)) => Err(format!(
            "entry filled but recovery did not pass: {recovery}"
        )),
        (Err(entry), Err(recovery)) => Err(format!(
            "entry evidence failed ({entry}); recovery also failed ({recovery})"
        )),
    }
}

fn recovery_acceptance_run() -> Check<()> {
    if env_required("IB_OPTION_ACCOUNTING_RECOVERY_OPT_IN")? != RECOVERY_OPT_IN {
        return Err("exact option-accounting recovery opt-in is required".into());
    }
    let settings = ConnectionSettings::from_env()?;
    let receipt_path = PathBuf::from(env_required("IB_OPTION_ACCOUNTING_RECEIPT")?);
    let close_limit = parse_price(&env_required("IB_OPTION_ACCOUNTING_CLOSE_LIMIT")?)?;
    let mut plan = read_plan(&receipt_path, &settings.expected_account, close_limit)?;
    let receipt = Receipt::open_append(&receipt_path)?;
    receipt.append(
        "recovery_invocation",
        "configured",
        json!({
            "close_limit_raw_price_scale": close_limit.to_string(),
            "fresh_reconciliation_required": true,
        }),
    );
    receipt.check()?;
    reconcile_and_close(&settings, receipt, &mut plan, None)
}

#[test]
#[ignore = "live destructive paper-only probe; requires the exact semantic opt-in and private credential/receipt files"]
fn one_long_option_entry_reconnect_and_close_accounting() {
    let _serial = LIVE_SERIAL
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if let Err(error) = entry_acceptance_run() {
        panic!("option accounting acceptance did not pass: {error}");
    }
}

#[test]
#[ignore = "live safety-only paper recovery; requires its distinct exact semantic opt-in and an existing private receipt"]
fn reconcile_and_close_only_receipt_owned_option_exposure() {
    let _serial = LIVE_SERIAL
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if let Err(error) = recovery_acceptance_run() {
        panic!("option accounting recovery did not pass: {error}");
    }
}

#[test]
fn operator_limits_are_exact_and_bounded() {
    assert_eq!(parse_price("0.01"), Ok(PRICE_SCALE / 100));
    assert_eq!(parse_price("1"), Ok(PRICE_SCALE));
    assert!(parse_price("0.00999999").is_err());
    assert!(parse_price("1.00000001").is_err());
    assert!(parse_price("-0.1").is_err());
}

#[test]
fn new_entry_is_call_only_while_existing_long_put_recovery_remains_available() {
    assert!(right_is_supported(
        "C",
        QualificationPurpose::NewLongCallEntry
    ));
    assert!(right_is_supported(
        "call",
        QualificationPurpose::NewLongCallEntry
    ));
    assert!(!right_is_supported(
        "P",
        QualificationPurpose::NewLongCallEntry
    ));
    assert!(!right_is_supported(
        "PUT",
        QualificationPurpose::NewLongCallEntry
    ));
    assert!(!right_is_supported(
        "",
        QualificationPurpose::NewLongCallEntry
    ));

    assert!(right_is_supported(
        "C",
        QualificationPurpose::ExistingLongRecovery
    ));
    assert!(right_is_supported(
        "put",
        QualificationPurpose::ExistingLongRecovery
    ));
}

#[test]
fn owned_fill_prices_and_premium_cash_use_the_qualified_multiplier() {
    let buy_limit = PRICE_SCALE / 2;
    assert!(fill_price_at_or_better("BOT", 0.49, buy_limit));
    assert!(fill_price_at_or_better("BUY", 0.5, buy_limit));
    assert!(!fill_price_at_or_better("BOT", 0.51, buy_limit));
    assert!(!fill_price_at_or_better("BOT", 0.0, buy_limit));
    assert!(!fill_price_at_or_better("BOT", f64::NAN, buy_limit));
    assert!(fill_price_at_or_better("SLD", 0.51, buy_limit));
    assert!(fill_price_at_or_better("SELL", 0.5, buy_limit));
    assert!(!fill_price_at_or_better("SLD", 0.49, buy_limit));

    assert_eq!(derived_premium_cash("BOT", 0.5), Ok(-50.0));
    assert_eq!(derived_premium_cash("SLD", 0.5), Ok(50.0));
    assert!(derived_premium_cash("BOT", 0.0).is_err());
    assert!(cash_values_correspond(-50.0, -50.0));
    assert!(!cash_values_correspond(-0.5, -50.0));
}

fn commission(exec_id: &str, amount: f64, currency: &str) -> CommissionAndFeesReport {
    CommissionAndFeesReport {
        exec_id: exec_id.into(),
        commission_and_fees: amount,
        currency: currency.into(),
        realized_pnl: f64::MAX,
        yield_amount: f64::MAX,
        yield_redemption_date: String::new(),
    }
}

#[test]
fn fee_completeness_requires_raw_amount_currency_and_callback_correspondence() {
    let capture = WireCapture::new();
    capture.session.store(7, Ordering::Release);
    let marker = capture.commission_marker();
    capture.note(&[
        (35, "U".into()),
        (6040, "60".into()),
        (17, "fill.01".into()),
        (6378, "1.25".into()),
        (6381, "USD".into()),
    ]);
    let native = capture.commissions_since(marker);
    assert_eq!(native.len(), 1);
    assert_eq!(native[0].amount.as_deref(), Some("1.25"));
    assert_eq!(native[0].currency.as_deref(), Some("USD"));
    assert_eq!(
        verify_commission_correspondence(
            &BTreeSet::from(["fill.01".to_string()]),
            &[commission("fill.01", 1.25, "USD")],
            &native,
        ),
        Ok(vec![VerifiedCommission {
            execution_id: "fill.01".into(),
            amount: 1.25,
            currency: "USD".into(),
        }])
    );

    let explicit_zero = [NativeCommission {
        session: 7,
        sequence: 2,
        execution_id: "zero.01".into(),
        amount: Some("0".into()),
        currency: Some("USD".into()),
    }];
    assert!(
        verify_commission_correspondence(
            &BTreeSet::from(["zero.01".to_string()]),
            &[commission("zero.01", 0.0, "USD")],
            &explicit_zero,
        )
        .is_ok()
    );

    let synthetic_zero = [NativeCommission {
        session: 7,
        sequence: 2,
        execution_id: "fill.01".into(),
        amount: None,
        currency: Some("USD".into()),
    }];
    assert!(
        verify_commission_correspondence(
            &BTreeSet::from(["fill.01".to_string()]),
            &[commission("fill.01", 0.0, "USD")],
            &synthetic_zero,
        )
        .is_err()
    );
    assert!(
        verify_commission_correspondence(
            &BTreeSet::from(["fill.01".to_string()]),
            &[commission("fill.01", 1.25, "EUR")],
            &native,
        )
        .is_err()
    );
    assert!(
        verify_commission_correspondence(
            &BTreeSet::from(["fill.01".to_string()]),
            &[commission("fill.01", 1.5, "USD")],
            &native,
        )
        .is_err()
    );
}

#[test]
fn native_fill_classifier_excludes_acks_history_lifecycle_and_foreign_accounts() {
    let base = |extra: &[(u32, &str)]| {
        let mut fields = vec![
            (35, "8".to_string()),
            (6008, "9001".to_string()),
            (17, "execution.01".to_string()),
            (150, "F".to_string()),
            (20, "0".to_string()),
            (32, "1".to_string()),
            (1, "DU_TEST".to_string()),
        ];
        fields.extend(
            extra
                .iter()
                .map(|(tag, value)| (*tag, (*value).to_string())),
        );
        fields
    };
    assert!(ordinary_native_execution(&base(&[]), 9001, "DU_TEST").is_some());

    let mut ack = base(&[]);
    field_replace(&mut ack, 150, "0");
    assert!(ordinary_native_execution(&ack, 9001, "DU_TEST").is_none());
    let mut history = base(&[]);
    field_replace(&mut history, 20, "3");
    assert!(ordinary_native_execution(&history, 9001, "DU_TEST").is_none());
    assert!(ordinary_native_execution(&base(&[(6809, "8")]), 9001, "DU_TEST").is_none());
    assert!(ordinary_native_execution(&base(&[]), 9001, "DU_OTHER").is_none());

    let mut correction = base(&[]);
    field_replace(&mut correction, 20, "1");
    assert_eq!(
        ordinary_native_execution(&correction, 9001, "DU_TEST").map(|row| row.2),
        Some(true)
    );
}

fn field_replace(fields: &mut [(u32, String)], tag: u32, value: &str) {
    fields
        .iter_mut()
        .find(|(candidate, _)| *candidate == tag)
        .unwrap()
        .1 = value.to_string();
}

#[test]
fn raw_connection_evidence_is_request_and_physical_session_scoped() {
    let capture = WireCapture::new();
    {
        let mut tracker = capture.tracker.lock().unwrap();
        tracker.position_completions.push((2, 1, "DU_TEST".into()));
        tracker.account_ends.push((1, 2, "AR.2".into()));
        tracker.account_ends.push((2, 3, "SR.Socket.1".into()));
        tracker.account_ends.push((2, 4, "AR.1".into()));
    }
    assert!(!capture.has_connection_image(2, "DU_TEST", "AR.2"));
    assert!(capture.has_connection_image(2, "DU_TEST", "AR.1"));
}

#[test]
fn raw_allowlist_excludes_auth_and_unreviewed_messages() {
    let fields = |values: &[(u32, &str)]| {
        values
            .iter()
            .map(|(tag, value)| (*tag, (*value).to_string()))
            .collect::<Vec<_>>()
    };
    assert!(!allowed_incoming(&fields(&[(35, "A")]), 9001));
    assert!(!allowed_incoming(&fields(&[(35, "U"), (6040, "5")]), 9001));
    assert!(!allowed_incoming(
        &fields(&[(35, "8"), (6008, "9002"), (32, "1")]),
        9001
    ));

    for message in [
        fields(&[(35, "UT")]),
        fields(&[(35, "U"), (6040, "60")]),
        fields(&[(35, "U"), (6040, "75")]),
        fields(&[(35, "UP")]),
        fields(&[(35, "EB")]),
        fields(&[(35, "8"), (32, "*")]),
        fields(&[(35, "8"), (6008, "9001"), (32, "1")]),
    ] {
        assert!(allowed_incoming(&message, 9001));
    }
}

#[test]
fn raw_portfolio_parser_accepts_a_singleton_row_without_group_markers() {
    let fields = [
        (35, "UP".to_string()),
        (6529, "AR.4".to_string()),
        (6008, "9001".to_string()),
        (6064, "1".to_string()),
        (6101, "0.5".to_string()),
    ];
    let rows = portfolio_rows(&fields, 4, 9, 9001, 8001);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].session, 4);
    assert_eq!(rows[0].sequence, 9);
    assert_eq!(rows[0].account_request, "AR.4");
    assert_eq!(rows[0].con_id, 9001);
    assert_eq!(rows[0].quantity.as_deref(), Some("1"));
    assert_eq!(rows[0].average_cost.as_deref(), Some("0.5"));
}

fn history_row(
    execution_id: &str,
    order_ref: &str,
    account: &str,
    con_id: i64,
    side: &str,
    shares: f64,
) -> OwnedHistoryRow {
    OwnedHistoryRow {
        execution_id: execution_id.into(),
        order_ref: order_ref.into(),
        account: account.into(),
        con_id,
        side: side.into(),
        shares,
        price: 0.5,
        pending_price_revision: false,
    }
}

#[test]
fn owned_history_projects_exact_buy_sell_and_deduplicates_identical_ids() {
    let owned = BTreeSet::from(["entry".to_string(), "close".to_string()]);
    let buy = history_row("buy.01", "entry", "DU_TEST", 9001, "BOT", 1.0);
    let sell = history_row("sell.01", "close", "DU_TEST", 9001, "SLD", 1.0);
    let unowned = history_row("", "other", "DU_OTHER", 17, "UNKNOWN", 99.0);
    let projection =
        project_owned_history(&[buy.clone(), buy, sell, unowned], &owned, "DU_TEST", 9001).unwrap();
    assert_eq!(projection.buys, QTY_SCALE);
    assert_eq!(projection.sells, QTY_SCALE);
    assert_eq!(projection.net_fixed, 0);
    assert_eq!(
        projection.execution_ids,
        BTreeSet::from(["buy.01".to_string(), "sell.01".to_string()])
    );
}

#[test]
fn owned_history_rejects_corrections_and_pending_revisions() {
    let owned = BTreeSet::from(["entry".to_string()]);
    assert!(
        project_owned_history(
            &[history_row("buy.02", "entry", "DU_TEST", 9001, "BOT", 1.0)],
            &owned,
            "DU_TEST",
            9001,
        )
        .is_err()
    );
    let mut pending = history_row("buy.01", "entry", "DU_TEST", 9001, "BOT", 1.0);
    pending.pending_price_revision = true;
    assert!(project_owned_history(&[pending], &owned, "DU_TEST", 9001).is_err());
}

#[test]
fn owned_history_rejects_foreign_identity_and_unsupported_side() {
    let owned = BTreeSet::from(["entry".to_string()]);
    for row in [
        history_row("buy.01", "entry", "DU_OTHER", 9001, "BOT", 1.0),
        history_row("buy.01", "entry", "DU_TEST", 9002, "BOT", 1.0),
        history_row("buy.01", "entry", "DU_TEST", 9001, "SSHORT", 1.0),
    ] {
        assert!(project_owned_history(&[row], &owned, "DU_TEST", 9001).is_err());
    }
}

#[test]
fn owned_history_rejects_oversells_and_more_than_one_unit() {
    let owned = BTreeSet::from(["entry".to_string(), "close".to_string()]);
    assert!(
        project_owned_history(
            &[history_row("sell.01", "close", "DU_TEST", 9001, "SLD", 1.0)],
            &owned,
            "DU_TEST",
            9001,
        )
        .is_err()
    );
    assert!(
        project_owned_history(
            &[history_row("buy.01", "entry", "DU_TEST", 9001, "BOT", 2.0)],
            &owned,
            "DU_TEST",
            9001,
        )
        .is_err()
    );
}

#[test]
fn known_owned_fills_cannot_disappear_from_a_later_history() {
    let required = BTreeSet::from(["buy.01".to_string()]);
    assert!(require_history_execution_ids(&BTreeSet::new(), &required).is_err());
    assert!(require_history_execution_ids(&required, &required).is_ok());
}

#[test]
fn receipt_execution_restore_is_exactly_owned_and_account_bound() {
    let owned = BTreeSet::from(["entry".to_string()]);
    let record = |order_ref: &str, account: &str| {
        json!({
            "contract": {"con_id": 9001},
            "execution": {
                "exec_id": "buy.01",
                "order_ref": order_ref,
                "account": account,
                "pending_price_revision": false,
            },
        })
    };
    assert_eq!(
        recorded_owned_execution_id(&record("entry", "DU_TEST"), &owned, "DU_TEST", 9001),
        Ok(Some("buy.01".into()))
    );
    assert_eq!(
        recorded_owned_execution_id(&record("other", "DU_OTHER"), &owned, "DU_TEST", 9001),
        Ok(None)
    );
    assert!(
        recorded_owned_execution_id(&record("entry", "DU_OTHER"), &owned, "DU_TEST", 9001).is_err()
    );
}

#[test]
fn post_fill_evidence_rejects_obsolete_account_streams() {
    let capture = WireCapture::new();
    capture.session.store(4, Ordering::Release);
    capture.option_con_id.store(9001, Ordering::Release);
    *capture.account_request.lock().unwrap() = "AR.4".into();
    let marker = (4, 0, 0, 0, 0);
    {
        let mut tracker = capture.tracker.lock().unwrap();
        tracker
            .account_frames
            .push((4, 1, "UM".into(), "AR.3".into()));
        tracker.option_execution_frames.push(NativeExecution {
            session: 4,
            sequence: 2,
            execution_id: "execution.01".into(),
            order_ref: Some("owned".into()),
            correction_status: false,
        });
        tracker.portfolio_rows.push(NativePortfolioRow {
            session: 4,
            sequence: 3,
            account_request: "AR.3".into(),
            con_id: 9001,
            quantity: Some("1".into()),
            average_cost: Some("0.5".into()),
            market_price: None,
            market_value: None,
            unrealized_pnl: None,
            realized_pnl: None,
        });
    }
    let ids = BTreeSet::from(["execution.01".to_string()]);
    assert!(!capture.post_fill_evidence(marker, 1.0, &ids, "owned"));

    {
        let mut tracker = capture.tracker.lock().unwrap();
        tracker
            .account_frames
            .push((4, 4, "UM".into(), "AR.4".into()));
        tracker.portfolio_rows.push(NativePortfolioRow {
            session: 4,
            sequence: 5,
            account_request: "AR.4".into(),
            con_id: 9001,
            quantity: Some("1".into()),
            average_cost: Some("0.5".into()),
            market_price: None,
            market_value: None,
            unrealized_pnl: None,
            realized_pnl: None,
        });
    }
    assert!(capture.post_fill_evidence(marker, 1.0, &ids, "owned"));
}

#[test]
fn low_level_order_uses_the_registered_nonzero_instrument() {
    let request = owned_limit_request(17, 37, Side::Buy, MIN_LIMIT, "owned".into());
    match request {
        OrderRequest::SubmitEx {
            instrument,
            qty,
            kind: OrderKind::Limit { price },
            attrs,
            ..
        } => {
            assert_eq!(instrument, 37);
            assert_eq!(qty, 1);
            assert_eq!(price, MIN_LIMIT);
            assert_eq!(attrs.order_ref, "owned");
        }
        _ => panic!("expected SubmitEx"),
    }
}
