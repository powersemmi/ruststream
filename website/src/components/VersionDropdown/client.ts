import {useEffect, useState} from 'react';
import {parseVersions, samePageIn, versionRoot, type VersionEntry} from './versions';

// Each version is a separate build, so the list comes from the shared file at run time: an
// older build lists the versions released after it.
export function usePublishedVersions(siteRoot: string): VersionEntry[] {
  const [versions, setVersions] = useState<VersionEntry[]>([]);
  useEffect(() => {
    let cancelled = false;
    fetch(`${siteRoot}versions.json`)
      .then((response) => (response.ok ? response.json() : []))
      .then((json: unknown) => {
        if (!cancelled) {
          setVersions(parseVersions(json));
        }
      })
      .catch(() => {
        // Without the file the dropdown shows only the version being read.
      });
    return () => {
      cancelled = true;
    };
  }, [siteRoot]);
  return versions;
}

// The same page may not exist in another version; asking first avoids landing on a 404.
export async function openSamePageIn(siteRoot: string, baseUrl: string, version: string): Promise<void> {
  const {pathname, hash} = window.location;
  const target = samePageIn(siteRoot, baseUrl, pathname, version);
  let exists = false;
  try {
    exists = (await fetch(target, {method: 'HEAD'})).ok;
  } catch {
    // An unreachable page is treated like a missing one.
  }
  window.location.assign(exists ? target + hash : versionRoot(siteRoot, version));
}
