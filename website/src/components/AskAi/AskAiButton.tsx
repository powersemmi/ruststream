import React, {type ReactNode} from 'react';
import clsx from 'clsx';
import {translate} from '@docusaurus/Translate';
import {openAskAi} from './events';
import styles from './styles.module.css';

interface Props {
  /** Set when the navbar renders the item in its mobile sidebar. */
  mobile?: boolean;
  className?: string;
}

export default function AskAiButton({mobile, className}: Props): ReactNode {
  const label = translate({id: 'askAi.button', message: 'Ask AI'});
  const button = (
    <button
      type="button"
      className={clsx(
        styles.navbarButton,
        mobile ? 'menu__link' : 'navbar__item navbar__link',
        className,
      )}
      onClick={() => openAskAi()}>
      {label}
    </button>
  );
  return mobile ? <li className="menu__list-item">{button}</li> : button;
}
