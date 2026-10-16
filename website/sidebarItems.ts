/**
 * The parts of a generated sidebar item this module reads; everything else passes through.
 */
export interface SidebarItem {
  type: string;
  id?: string;
  label?: string;
  link?: {type: string; id?: string};
  items?: SidebarItem[];
  translatable?: true;
}

/**
 * Keeps the labels of a generated sidebar translatable.
 *
 * Docusaurus offers a label for translation when the sidebar configuration writes it, and
 * a generated category's own label. A generated subcategory that has only its index page so far
 * renders as a link to that page, under the category's label, and Docusaurus offers that label
 * for no translation, so a localized site shows it in English. Here such a subcategory becomes
 * that link with its label marked translatable, and a page named in `labels` gets that label,
 * marked the same way.
 */
export function translatableLabels<Item extends SidebarItem>(
  items: Item[],
  labels: Record<string, string> = {},
): Item[] {
  return items.map((item): Item => {
    if (item.type === 'category' && item.items) {
      if (item.items.length === 0 && item.link?.type === 'doc' && item.label) {
        return {type: 'doc', id: item.link.id, label: item.label, translatable: true} as Item;
      }
      return {...item, items: translatableLabels(item.items, labels)};
    }
    if (item.type === 'doc' && item.id && labels[item.id]) {
      return {...item, label: labels[item.id], translatable: true};
    }
    return item;
  });
}
