# Security and deployment boundary

`experiment-queue` executes submitted project commands; it is not a sandbox.
Registering a project authorizes arbitrary code from its admitted Git commits to
run with the queue service account's filesystem, process, network, and credential
access.

Logical volumes and declared artifacts are optional provenance features. Their
absence does not restrict project code to the checkout: the process retains the
service account's ordinary host access. Use Unix ownership and permissions—not
Project.yaml—as the containment boundary.

Production deployments should:

- use a dedicated non-root service account with least privilege;
- keep the configured state directory private to that account;
- keep credentials out of project manifests, cards, logs, and receipts;
- bind the web application to loopback unless it is protected by authenticated
  private networking and TLS;
- treat logs and captured environment/provenance as potentially sensitive;
- authorize artifact and log paths server-side rather than trusting browser
  filtering;
- run only one scheduler for a queue instance and GPU pool.

GPU ownership is cooperative on an unmanaged host. Processes outside the queue
can still use a device. Manual preemption is safe only for jobs implementing the
versioned cooperative checkpoint protocol; it is not generic process suspension.

Report a suspected vulnerability privately to the repository owner rather than
opening a public issue containing credentials, state snapshots, or logs.
