// The landing page's numbers come from the core's published benchmark results, read at build
// time, so the page always quotes the last measurement committed to the branch it is built from.

export interface Results {
  crate_version: string;
  core_version: string;
  measured_at: string;
  environment: {cpu: string};
  code: Array<{name: string; framework: {instructions: number; allocations: number}}>;
}

/** The scenarios the landing page quotes, in the order it shows them. */
export const HEADLINE_SCENARIOS = [
  'consume, JSON decode into a small struct',
  'consume on the byte lane, no codec',
  'consume through a middleware stack of four',
  'reply to a transport that reads the payload',
] as const;

export type HeadlineScenario = (typeof HEADLINE_SCENARIOS)[number];

export interface NumbersRow {
  scenario: HeadlineScenario;
  instructions: string;
  allocations: string;
}

export interface Numbers {
  version: string;
  measuredAt: string;
  cpu: string;
  rows: NumbersRow[];
}

/**
 * Picks the headline scenarios out of the results and formats them for `locale`.
 *
 * Throws when a scenario is missing, so a renamed benchmark fails the site build instead of
 * leaving an empty cell on the page.
 */
export function selectNumbers(results: Results, locale: string): Numbers {
  const whole = new Intl.NumberFormat(locale, {maximumFractionDigits: 0});
  const exact = new Intl.NumberFormat(locale, {maximumFractionDigits: 3});
  const rows = HEADLINE_SCENARIOS.map((scenario) => {
    const found = results.code.find((entry) => entry.name === scenario);
    if (!found) {
      throw new Error(`The benchmark results have no scenario named '${scenario}'`);
    }
    return {
      scenario,
      instructions: whole.format(found.framework.instructions),
      allocations: exact.format(found.framework.allocations),
    };
  });
  return {
    version: results.crate_version,
    measuredAt: results.measured_at,
    cpu: results.environment.cpu,
    rows,
  };
}
