import React, {type ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import Translate from '@docusaurus/Translate';
import Heading from '@theme/Heading';
import type {Props} from '@theme/NotFound/Content';

// The site's 404 page answers every missing address under /ruststream/, an old version's and a
// moved page's included, so it sends the reader back into the current documentation.
export default function NotFoundContent({className}: Props): ReactNode {
  return (
    <main className={clsx('container margin-vert--xl', className)}>
      <div className="row">
        <div className="col col--6 col--offset-3">
          <Heading as="h1" className="hero__title">
            <Translate id="theme.NotFound.title" description="The title of the 404 page">
              Page Not Found
            </Translate>
          </Heading>
          <p>
            <Translate id="theme.NotFound.p1" description="The first paragraph of the 404 page">
              We could not find what you were looking for.
            </Translate>
          </p>
          <p>
            <Translate
              id="notFound.moved"
              description="The 404 page's way back into the documentation"
              values={{
                home: (
                  <Link to="/">
                    <Translate id="notFound.moved.home">
                      the start page of the documentation
                    </Translate>
                  </Link>
                ),
              }}>
              {'The page may have moved when the documentation was reorganized. Continue from {home} or search for it.'}
            </Translate>
          </p>
        </div>
      </div>
    </main>
  );
}
