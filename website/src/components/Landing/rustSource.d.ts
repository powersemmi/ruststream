// A `.rs` file imported from a component is its source text (see `rustSourceImports` in the
// site configuration).
declare module '*.rs' {
  const source: string;
  export default source;
}
