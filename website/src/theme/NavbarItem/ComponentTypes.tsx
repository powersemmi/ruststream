import ComponentTypes from '@theme-original/NavbarItem/ComponentTypes';
import AskAiButton from '@site/src/components/AskAi/AskAiButton';

// Navbar item types of the site, used in the navbar config as `type: 'custom-<name>'`.
export default {
  ...ComponentTypes,
  'custom-askAi': AskAiButton,
};
