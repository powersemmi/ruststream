import React, {type ReactNode} from 'react';
import styles from './landing.module.css';

interface Props {
  id: string;
  eyebrow: ReactNode;
  title: ReactNode;
  lede?: ReactNode;
}

/** The centered heading every landing section opens with. */
export default function SectionHead({id, eyebrow, title, lede}: Props): ReactNode {
  return (
    <div className={styles.sectionHead}>
      <span className={styles.eyebrow}>{eyebrow}</span>
      <h2 id={id} className={styles.title}>
        {title}
      </h2>
      {lede && <p className={styles.lede}>{lede}</p>}
    </div>
  );
}
