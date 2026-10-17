import {describe, expect, it} from 'vitest';
import {verificationTags} from './siteVerification';

describe('verificationTags', () => {
  it('writes a tag for each search console token the build was given', () => {
    expect(
      verificationTags({
        DOCS_GOOGLE_SITE_VERIFICATION: 'abc-DEF_1',
        DOCS_BING_SITE_VERIFICATION: 'B1NG',
      }),
    ).toEqual([
      {tagName: 'meta', attributes: {name: 'google-site-verification', content: 'abc-DEF_1'}},
      {tagName: 'meta', attributes: {name: 'msvalidate.01', content: 'B1NG'}},
    ]);
  });

  it('writes nothing for a token left empty', () => {
    expect(verificationTags({DOCS_GOOGLE_SITE_VERIFICATION: ''})).toEqual([]);
    expect(verificationTags({})).toEqual([]);
  });

  it('refuses a token that is not letters, digits, "-" and "_"', () => {
    expect(() => verificationTags({DOCS_GOOGLE_SITE_VERIFICATION: 'a"><script>'})).toThrow(
      /DOCS_GOOGLE_SITE_VERIFICATION/,
    );
  });
});
