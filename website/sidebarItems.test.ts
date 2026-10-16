import {describe, expect, it} from 'vitest';
import {translatableLabels} from './sidebarItems';

describe('translatableLabels', () => {
  it('keeps a subcategory with pages a category', () => {
    const category = {
      type: 'category' as const,
      label: 'Codecs',
      link: {type: 'doc' as const, id: 'components/subscribers/codecs/index'},
      items: [{type: 'doc' as const, id: 'components/subscribers/codecs/json'}],
    };
    expect(translatableLabels([category])).toEqual([category]);
  });

  it('turns a subcategory with no pages yet into its index under the same label, translatable', () => {
    expect(
      translatableLabels([
        {
          type: 'category',
          label: 'Subscribers',
          link: {type: 'doc', id: 'components/subscribers/index'},
          items: [
            {
              type: 'category',
              label: 'Dispatch',
              link: {type: 'doc', id: 'components/subscribers/dispatch/index'},
              items: [],
            },
          ],
        },
      ]),
    ).toEqual([
      {
        type: 'category',
        label: 'Subscribers',
        link: {type: 'doc', id: 'components/subscribers/index'},
        items: [
          {
            type: 'doc',
            id: 'components/subscribers/dispatch/index',
            label: 'Dispatch',
            translatable: true,
          },
        ],
      },
    ]);
  });

  it('labels the overview of the section, translatable', () => {
    expect(
      translatableLabels([{type: 'doc', id: 'components/index'}], {
        'components/index': 'Overview',
      }),
    ).toEqual([{type: 'doc', id: 'components/index', label: 'Overview', translatable: true}]);
  });
});
