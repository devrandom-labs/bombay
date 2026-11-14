# Remote Counter

Run a Counter and its customer as separate programs using Bombay's public
remote capabilities. The customer requests a correlated increment, reads the
value, then requests an authorized stop. One Counter export serves all three
operations; both programs use Node-owned networking and cleanup.

This example uses an explicitly selected **finite test identity**. A trusted
operator supplies exact request/reply byte and proof pairs for the actual
process bindings. `production-required` refuses this provider before opening
a Node. Production identity integration belongs to Selo.

## Run

From the repository root, the process test provisions both programs and runs
the complete exchange with the supplied test TLS credentials:

```sh
nix develop -c cargo test --locked -p bombay-example-remote-counter --test processes
nix develop -c cargo test --locked --release -p bombay-example-remote-counter --test processes
```

To run the roles yourself, first start a trusted assignment operator on a local
Unix socket. Then start the server followed by the customer:

```sh
nix develop -c cargo run --locked -p bombay-example-remote-counter -- \
  serve test localhost:7447 /path/to/certificates /path/to/assignment.sock
nix develop -c cargo run --locked -p bombay-example-remote-counter -- \
  call test localhost:7447 /path/to/certificates /path/to/assignment.sock 20
```

Both roles take provider mode, TLS host/port, certificate directory and
assignment socket after the role name. Only `call` also takes a positive wait
duration in seconds. The directory supplies
`root-ca.pem`, `recipient-certificate.pem`, `recipient-private-key.pem`,
`caller-certificate.pem` and `caller-private-key.pem`. The server listens;
the customer connects. Use credentials issued for the configured DNS name.
The assignment socket requires a Unix host.

## Test assignment

Each program connects once to the assignment socket after opening its Node.
It writes one newline-terminated JSON object containing `role` (`serve` or
`call`), its actual `runtime` and its actual `actor` address. These values
describe the process; the trusted operator decides its authority.

The operator returns one JSON line, at most262144 bytes, containing `profile`,
`revision`, both `caller` and `receiver` bindings, three `grants` and six
`records`. The profile is `remote-counter-finite-pairs-v1`. Each record contains
the exact `protected` bytes and `proof`. Supply one request/reply pair for each
of Increment, Read and Stop, with distinct request correlations and matching
metadata. The programs validate the complete assignment before declaring
their remote capabilities. The process test shows this assignment contract.

The provider accepts only those exact configured pairs. Assignment failure is
a startup refusal. The operator must respond or close its socket connection;
the actor timeout starts after application startup. No authority is learned
from received peer claims.

The server explicitly discharges completed ordinary refusals so invalid
requests do not consume its request capacity forever. Issued request keys and
provider panic evidence stay retained; this finite example does not provide an
unbounded request cache or an aggregate memory bound.

## Results

On success the customer prints:

```text
counter value: 1
counter stopped
```

The server prints `counter stopped`. Both exit0 after their joined execution
and cleanup checks succeed. The wait duration selects customer receive
inactivity and native query expiry. An unanswered operation stops the customer
with `counter uncertain: Increment`, `Read` or `Stop` and exit2. It does not
resend an uncertain operation or claim that it never executed. A refusal or
execution failure prints a bounded error and exits1.

Failure output reports the first applicable refusal. Complete native outcomes
remain with the runtime owners until application scope exit; this example's
exit message is a summary of that execution.

The actor remains pure: requests and the customer's useful result are typed
Actions. The application awaits its result while Node owns routine resource
lifetimes and retirement ordering. Tests keep provisioning, process guards and
fault assertions outside application modules.
