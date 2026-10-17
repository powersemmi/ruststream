import {describe, expect, it} from 'vitest';
import {movedSection} from './anchors';

describe('a section that moved to a page of its own', () => {
  it('opens the page that took the section over', () => {
    expect(movedSection('#methodology')).toBe('methodology/');
    expect(movedSection('#what-the-numbers-are')).toBe('numbers/');
    expect(movedSection('#publishing-results')).toBe('publishing/');
  });

  it('keeps a subsection anchor on its new page', () => {
    expect(movedSection('#the-three-loops')).toBe('methodology/#the-three-loops');
    expect(movedSection('#the-cost-of-the-code')).toBe('numbers/#the-cost-of-the-code');
    expect(movedSection('#the-document')).toBe('publishing/#the-document');
  });

  it('leaves an anchor of the results page where it is', () => {
    expect(movedSection('#against-a-raw-client')).toBeUndefined();
    expect(movedSection('')).toBeUndefined();
  });
});
