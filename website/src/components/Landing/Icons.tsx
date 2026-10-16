import React, {type ReactNode} from 'react';

// Line icons for the next-step cards, drawn for the site on a 24-unit grid. They take the text
// color, so they follow the theme.
function Icon({children}: {children: ReactNode}): ReactNode {
  return (
    <svg
      viewBox="0 0 24 24"
      width="22"
      height="22"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.75"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      focusable="false">
      {children}
    </svg>
  );
}

export const DownloadIcon = () => (
  <Icon>
    <path d="M12 4v11M7.5 10.5 12 15l4.5-4.5M5 19h14" />
  </Icon>
);

export const RocketIcon = () => (
  <Icon>
    <path d="M14.5 4.5c2.5-.8 4.5-.8 5-.5.3.5.3 2.5-.5 5l-6 6-4-4 5.5-6.5Z" />
    <path d="M9 11 5.5 10.5 8 8h4M13 15l.5 3.5L16 16v-4M6.5 17.5 4 20" />
    <circle cx="15.5" cy="8.5" r="1.25" />
  </Icon>
);

export const BookIcon = () => (
  <Icon>
    <path d="M4 5.5C6.5 4.5 9.5 4.5 12 6c2.5-1.5 5.5-1.5 8-.5v13c-2.5-1-5.5-1-8 .5-2.5-1.5-5.5-1.5-8-.5v-13Z" />
    <path d="M12 6v13" />
  </Icon>
);

export const FlaskIcon = () => (
  <Icon>
    <path d="M9.5 3.5h5M10 3.5v6L4.8 18.2A1.5 1.5 0 0 0 6.1 20.5h11.8a1.5 1.5 0 0 0 1.3-2.3L14 9.5v-6" />
    <path d="M7.5 14.5h9" />
  </Icon>
);

export const GlobeIcon = () => (
  <Icon>
    <circle cx="12" cy="12" r="8.5" />
    <path d="M3.5 12h17M12 3.5c2.5 2.5 3.5 5.5 3.5 8.5s-1 6-3.5 8.5c-2.5-2.5-3.5-5.5-3.5-8.5s1-6 3.5-8.5Z" />
  </Icon>
);

export const BracesIcon = () => (
  <Icon>
    <path d="M8.5 4.5c-2 0-2.5 1-2.5 2.5v2.5c0 1.5-.8 2.5-2 2.5 1.2 0 2 1 2 2.5V17c0 1.5.5 2.5 2.5 2.5M15.5 4.5c2 0 2.5 1 2.5 2.5v2.5c0 1.5.8 2.5 2 2.5-1.2 0-2 1-2 2.5V17c0 1.5-.5 2.5-2.5 2.5" />
  </Icon>
);

export const GaugeIcon = () => (
  <Icon>
    <path d="M4.5 17a8.5 8.5 0 1 1 15 0" />
    <path d="M12 13.5 16 9M12 13.5a1.5 1.5 0 1 0 0 .01" />
  </Icon>
);

export const DocumentIcon = () => (
  <Icon>
    <path d="M6.5 3.5h7l4 4v13h-11v-17Z" />
    <path d="M13.5 3.5v4h4M9 12h6M9 15h6M9 18h3.5" />
  </Icon>
);
