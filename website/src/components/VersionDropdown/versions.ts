// The versions file sits at the site root, one level above every version directory, in the
// format mike wrote it: newest first, a development version (`next`) above the releases,
// `aliases` naming the directories that mirror a version.
export interface VersionEntry {
  version: string;
  title: string;
  aliases: string[];
}

export function parseVersions(json: unknown): VersionEntry[] {
  if (!Array.isArray(json)) {
    return [];
  }
  return json.flatMap((entry: unknown) => {
    if (typeof entry !== 'object' || entry === null) {
      return [];
    }
    const {version, title, aliases} = entry as Record<string, unknown>;
    if (typeof version !== 'string') {
      return [];
    }
    return [
      {
        version,
        title: typeof title === 'string' ? title : version,
        aliases: Array.isArray(aliases)
          ? aliases.filter((alias): alias is string => typeof alias === 'string')
          : [],
      },
    ];
  });
}

/** The directory a build is served from: `0.7` for `/ruststream/0.7/`, or an alias. */
// A translated build's base URL carries its language after the version (`next/ru/`); the
// version is the first segment.
function directoryOf(siteRoot: string, baseUrl: string): string {
  return baseUrl.slice(siteRoot.length).split('/')[0] ?? '';
}

export function currentEntry(
  versions: VersionEntry[],
  siteRoot: string,
  baseUrl: string,
): VersionEntry | undefined {
  const directory = directoryOf(siteRoot, baseUrl);
  return (
    versions.find((entry) => entry.version === directory) ??
    versions.find((entry) => entry.aliases.includes(directory))
  );
}

/** The newest release: the entry the `latest` alias points at (`next` is listed above it). */
export function latestEntry(versions: VersionEntry[]): VersionEntry | undefined {
  return versions.find((entry) => entry.aliases.includes('latest'));
}

export function versionRoot(siteRoot: string, version: string): string {
  return `${siteRoot}${version}/`;
}

/** The address of the current page in another version, language included. */
export function samePageIn(
  siteRoot: string,
  baseUrl: string,
  pathname: string,
  version: string,
): string {
  const root = versionRoot(siteRoot, version);
  return pathname.startsWith(baseUrl) ? root + pathname.slice(baseUrl.length) : root;
}
