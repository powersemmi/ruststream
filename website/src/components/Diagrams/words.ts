/** The words of one diagram, written on its page so each language breaks its own lines. */
export type Words = Record<string, string | string[]>;

/** A box's lines: a list as the page broke it, or a single line. */
export const lines = (words: Words, key: string): string[] => {
  const value = words[key];
  if (value === undefined) {
    // A missing word is a page out of step with its diagram; the build fails on it.
    throw new Error(`The diagram has no words for '${key}'`);
  }
  return typeof value === 'string' ? [value] : value;
};

/** A single word of the page: a link label, a title, a summary. */
export const word = (words: Words, key: string): string => lines(words, key).join(' ');
