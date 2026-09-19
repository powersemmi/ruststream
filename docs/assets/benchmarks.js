/*
 * Renders the two summary tables on the Benchmarks page: what the framework costs against a raw
 * client, which every broker crate measures on its own broker, and what its code costs per
 * message, which the core and every broker crate measure on themselves.
 *
 * The numbers are fetched in the reader's browser instead of being copied into this repository:
 * a broker remeasures on its own schedule, and a copy here would be stale from the moment it
 * landed. Every broker site is served from the same origin as this one, so this is a same-origin
 * request; a preview served from localhost is not, and falls back to the "not published yet"
 * line like a broker that has published nothing.
 *
 * Prose is never written here. The page carries every label as JSON on the container, so the
 * translated pages control their own wording.
 */

(() => {
  "use strict";

  // The catalog, not the results: which sites to ask. A broker whose fetch fails for any reason
  // (nothing published yet, an unreadable document, no network) is listed as pending rather than
  // dropped, so a broken publish is visible instead of silently absent.
  const BROKERS = [
    { name: "NATS", repo: "ruststream-nats" },
    { name: "Redis", repo: "ruststream-fred" },
    { name: "RabbitMQ", repo: "ruststream-lapin" },
    { name: "Kafka", repo: "ruststream-rdkafka" },
    { name: "AMQP 1.0", repo: "ruststream-amqp" },
    { name: "Google Cloud Pub/Sub", repo: "ruststream-gcp-pubsub" },
    { name: "AWS SQS / SNS", repo: "ruststream-sqs-sns" },
    { name: "Apache Pulsar", repo: "ruststream-pulsar" },
    { name: "MQTT 5", repo: "ruststream-rumqttc" },
    { name: "ZeroMQ", repo: "ruststream-zeromq" },
    { name: "Stream files / stdio", repo: "ruststream-sea-file" },
    { name: "AWS Kinesis", repo: "ruststream-kinesis" },
  ];

  // The core measures no broker of its own, so it appears in the second table only.
  const CORE = { name: "RustStream", repo: "ruststream" };

  const SITE = "https://powersemmi.github.io/";
  // `mike set-default latest` makes the site root a redirect page, so the versioned alias is
  // part of the stable path rather than an implementation detail of the deploy.
  const RESULTS = "/latest/benchmarks/results.json";
  const PAGE = "/latest/benchmarks/";
  // Schema 2 added the `code` section; a schema 1 document still renders in the first table.
  const SCHEMAS = [1, 2];
  const TIMEOUT_MS = 8000;

  const text = (tag, value) => {
    const node = document.createElement(tag);
    node.textContent = value;
    return node;
  };

  const link = (href, label) => {
    const node = text("a", label);
    node.href = href;
    return node;
  };

  async function load(broker) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
    try {
      const response = await fetch(SITE + broker.repo + RESULTS, { signal: controller.signal });
      if (!response.ok) {
        return null;
      }
      const results = await response.json();
      // A future schema revision may reorder or retype fields, and rendering it as if it were
      // one of these would print wrong numbers rather than no numbers.
      return results && SCHEMAS.includes(results.schema) ? results : null;
    } catch {
      return null;
    } finally {
      clearTimeout(timer);
    }
  }

  const number = (value, lang) =>
    typeof value === "number" ? value.toLocaleString(lang, { maximumFractionDigits: 1 }) : "-";

  function side(measurement, unit, lang) {
    if (!measurement) {
      return "-";
    }
    const median = number(measurement.median, lang) + " " + unit;
    if (typeof measurement.min !== "number" || typeof measurement.max !== "number") {
      return median;
    }
    return median + " (" + number(measurement.min, lang) + "-" + number(measurement.max, lang) + ")";
  }

  function overhead(scenario, labels) {
    // The honesty rule of the methodology, enforced where it is read: a difference smaller than
    // the run-to-run spread is a verdict, never a percentage.
    let value =
      scenario.verdict === "indistinguishable"
        ? labels.indistinguishable
        : (scenario.overhead_percent >= 0 ? "+" : "") + scenario.overhead_percent + "%";
    if (scenario.broker_bound) {
      value += " (" + labels.brokerBound + ")";
    }
    return value;
  }

  function table(published, labels, lang) {
    const element = document.createElement("table");
    const head = element.createTHead().insertRow();
    for (const column of [labels.broker, labels.scenario, labels.raw, labels.framework, labels.overhead]) {
      head.appendChild(text("th", column));
    }
    const body = element.createTBody();
    for (const { broker, results } of published) {
      for (const [index, scenario] of results.scenarios.entries()) {
        const row = body.insertRow();
        // The broker names one row of its own however many scenarios it measured.
        row.appendChild(text("td", index === 0 ? broker.name : ""));
        row.appendChild(text("td", scenario.name));
        row.appendChild(text("td", side(scenario.raw, scenario.unit, lang)));
        row.appendChild(text("td", side(scenario.framework, scenario.unit, lang)));
        row.appendChild(text("td", overhead(scenario, labels)));
      }
    }
    return element;
  }

  function code(published, labels, lang) {
  const element = document.createElement("table");
  const head = element.createTHead().insertRow();
  const columns = [
    labels.crate,
    labels.scenario,
    labels.instructions,
    labels.allocations,
    labels.cold,
  ];
  for (const column of columns) {
    head.appendChild(text("th", column));
  }
  const body = element.createTBody();
  for (const { broker, results } of published) {
    for (const [index, scenario] of results.code.entries()) {
      const row = body.insertRow();
      // One name per crate however many scenarios it measured, as in the table above.
      row.appendChild(text("td", index === 0 ? broker.name : ""));
      row.appendChild(text("td", scenario.name));
      row.appendChild(text("td", number(scenario.framework.instructions, lang)));
      row.appendChild(text("td", number(scenario.framework.allocations, lang)));
      // Two numbers in one cell: what starting cost in instructions, and in allocations.
      row.appendChild(
        text(
          "td",
          scenario.cold
            ? number(scenario.cold.instructions, lang) + " / " + number(scenario.cold.allocations, lang)
            : "-",
        ),
      );
    }
  }
  return element;
}

function provenance(published, labels) {
    const list = document.createElement("ul");
    for (const { broker, results } of published) {
      const environment = results.environment || {};
      const parts = [
        results.crate + " " + results.crate_version + " on ruststream " + results.core_version,
        environment.cpu,
        environment.os,
        environment.broker,
        labels.measured + " " + results.measured_at,
      ].filter(Boolean);
      const item = document.createElement("li");
      item.appendChild(text("strong", broker.name));
      item.appendChild(document.createTextNode(" - " + parts.join(", ") + ". "));
      item.appendChild(link(SITE + broker.repo + PAGE, labels.details));
      list.appendChild(item);
    }
    return list;
  }

  // The machine a set of code numbers was taken on, in the order the fields are documented. Values
// come from the document as they were written; the page adds no words of its own to them.
const MACHINE = [
  "cpu",
  "architecture",
  "cpu_frequency",
  "cores",
  "memory",
  "memory_speed",
  "os",
  "rustc",
  "valgrind",
  "profile",
  "features",
];

function machine(published, labels) {
  const list = document.createElement("ul");
  for (const { broker, results } of published) {
    const environment = results.environment || {};
    const parts = MACHINE.map((field) => environment[field]).filter(Boolean);
    const item = document.createElement("li");
    item.appendChild(text("strong", broker.name));
    item.appendChild(
      document.createTextNode(
        " - " + parts.join(", ") + ". " + labels.measured + " " + results.measured_at + ".",
      ),
    );
    list.appendChild(item);
  }
  return list;
}

function renderCode(container, labels, lang, loaded) {
  const published = loaded.filter((entry) => entry.results && entry.results.code?.length);
  container.replaceChildren();
  if (published.length) {
    container.appendChild(code(published, labels, lang));
    container.appendChild(machine(published, labels));
  } else {
    container.appendChild(text("p", labels.pending.replace("{brokers}", CORE.name)));
  }
}

function render(container, labels, lang, loaded) {
    const published = loaded.filter((entry) => entry.results && entry.results.scenarios?.length);
    const pending = loaded.filter((entry) => !published.includes(entry));

    container.replaceChildren();
    if (published.length) {
      container.appendChild(table(published, labels, lang));
      container.appendChild(provenance(published, labels));
    }
    if (pending.length) {
      // The label is a template rather than a prefix: spacing and punctuation around the list
      // differ by language, and only the translated page knows which ones it wants.
      const names = pending.map((entry) => entry.broker.name).join(", ");
      container.appendChild(text("p", labels.pending.replace("{brokers}", names)));
    }
  }

  async function main() {
    const container = document.getElementById("benchmark-results");
    const codeContainer = document.getElementById("benchmark-code");
    if (!container && !codeContainer) {
      return;
    }
    const lang = document.documentElement.lang || "en";
    // One label set per page, carried by whichever container the page put it on.
    const source = container?.dataset.benchmarkLabels ? container : codeContainer;
    const labels = JSON.parse(source.dataset.benchmarkLabels);
    for (const element of [container, codeContainer]) {
      element?.replaceChildren(text("p", labels.loading));
    }
    const loaded = await Promise.all(
      [...BROKERS, CORE].map(async (broker) => ({ broker, results: await load(broker) })),
    );
    // The core publishes no pair measurement, so it is not one of the crates the first table
    // waits for; it leads the second one.
    const brokers = loaded.filter((entry) => entry.broker !== CORE);
    if (container) {
      render(container, labels, lang, brokers);
    }
    if (codeContainer) {
      const core = loaded.filter((entry) => entry.broker === CORE);
      renderCode(codeContainer, labels, lang, [...core, ...brokers]);
    }
  }

  // Material swaps page content without a reload, so the table is built on every navigation
  // rather than once per document.
  if (window.document$) {
    window.document$.subscribe(main);
  } else {
    document.addEventListener("DOMContentLoaded", main);
  }
})();
