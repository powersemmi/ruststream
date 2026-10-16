// The landing page shows files of `examples/` as they are, read at build time. The only lines it
// drops are the snippet markers the documentation pages cut the files with.
const MARKER = /^\s*\/\/ --8<-- \[(start|end):[\w-]+\]\s*$/;

/** The example's text with the snippet markers removed and no doubled blank lines. */
export function exampleSource(raw: string): string {
  const lines = raw.split('\n').filter((line) => !MARKER.test(line));
  return lines
    .filter((line, index) => line.trim() !== '' || lines[index - 1]?.trim() !== '')
    .join('\n')
    .trim();
}
