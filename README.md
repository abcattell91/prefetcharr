# prefetcharr

Have [Sonarr][sonarr] automatically fetch the next episodes of the show you’re
watching on [Jellyfin][jellyfin]/[Emby][emby]/[Plex][plex].

## Details

_prefetcharr_ periodically polls your media server for active playback sessions
of TV shows.
It then checks whether a configured number of successive episodes is available.
If there are episodes missing, it asks _Sonarr_ to search for them.
Depending on the configuration, it searches the missing episodes individually or
tries to fetch all seasons that contain them.
If there are no more seasons left, the series is monitored for new seasons
instead.

A search does not guarantee an episode actually turns up — a release may be
unobtainable, or the download may fail.  
While you keep watching a series, _prefetcharr_ therefore re-checks whether the
episodes it searched for have a file yet, and searches again for those that do
not.  
Episodes _Sonarr_ is currently downloading are left alone.

## Build and install

To install, first ensure Rust is installed on your system by following the
instructions at [Install Rust][rust], then run:
```
cargo install --git https://github.com/p-hueber/prefetcharr
```

Or with docker compose:
```yml
services:
  prefetcharr:
    image: phueber/prefetcharr:latest
    container_name: prefetcharr
    user: 1000:1000
    environment:
      - |
        PREFETCHARR_CONFIG=
        # Start of the configuration in TOML format.
       
        interval = 900           # Polling interval in seconds
        log_dir = "/log"         # Logging directory
        log_level = "Debug"      # `Trace`, `Debug`, `Info`, `Warn` or `Error`
        prefetch_num = 2         # Number of episodes to make available in advance
        request_seasons = true   # Always request full seasons to prefer season packs
        append_to_queue = false  # Experimental: Append upcoming episodes to the player's active queue.
                                 # Not supported by all clients. Not compatible with Tautulli.
        retry_unavailable = true # Search again for prefetched episodes that never turned up
        retry_interval = 1800    # Minimum seconds between two searches for the same episode
        connection_retries = 6   # Number of retries for the initial connection probing
        dry_run = false          # Optional: Log every change to Sonarr instead of applying it

        [media_server]
        type = "Jellyfin"                       # `Jellyfin`, `Emby`, `Plex` or `Tautulli`
        url = "http://example.com/jellyfin"     # Jellyfin/Emby/Plex/Tautulli baseurl
        api_key = "<YOUR KEY HERE>"             # Jellyfin/Emby/Tautulli API key or plex server token
        # users = [ "John", "12345", "Axel F" ] # Optional: Only monitor sessions for specific user IDs or names
        # libraries = [ "TV Shows", "Anime" ]   # Optional: Only monitor sessions for specific libraries

        [[sonarr]]
        # name = "tv"                     # Optional: Name used to tell instances apart in the logs
        url = "http://example.com/sonarr" # Sonarr baseurl
        api_key = "<YOUR KEY HERE>"       # Sonarr API key
        # exclude_tag = "no_prefetch"     # Optional: Exclude series by tag
        # libraries = [ "TV Shows" ]      # Optional: Only handle sessions from these libraries

        # Optional: Upgrade the quality of upcoming episodes for specific users.
        # See "Per-user quality boost" below.
        #   [[sonarr.boost]]
        #   users = [ "John", "12345" ]      # User IDs or names this rule applies to
        #   quality_profile = "HQ-1080p"     # Sonarr quality profile to switch the series to
        #   prefetch_num = 5                 # Optional: Episodes to upgrade in advance
        #   tag = "prefetcharr-boosted"      # Optional: Tag applied to boosted series
        #   search_cooldown = 43200          # Optional: Don't search an episode again within this many seconds

        # Optional: Further Sonarr instances, e.g. a dedicated anime one.
        # [[sonarr]]
        # name = "anime"
        # url = "http://example.com/sonarr-anime"
        # api_key = "<YOUR KEY HERE>"
        # libraries = [ "Anime" ]

    volumes:
      - /path/to/log/dir:/log
      # Keep the config in a file instead of PREFETCHARR_CONFIG
      # - /path/to/config.toml:/config

```

## Configuration

The configuration is written in [TOML][toml] format. When running `prefetcharr`
directly, you can pass the path of the configuration file using the `--config`
command-line flag. For the Docker container, provide the entire configuration
via the `PREFETCHARR_CONFIG` environment variable.
A complete example can be found in the installation instructions for
`docker-compose` above.

### API keys

_prefetcharr_ needs two different API keys to do its job.

#### `sonarr.api_key`

Go to `Settings` -> `General` -> `Security` and copy the API key.

#### `media_server.api_key`

The key to use and how to obtain it differs on the type of media server you use:

#### Jellyfin

Log in as an administrator and go to `Administration` -> `Dashboard` ->
`Advanced` -> `Api Keys`. Add a new key or use an existing one.

#### Emby

Log in as an administrator, click on the gear on the top right and go to
`Advanced` -> `Api Keys`. Add a new key or use an existing one.

#### Plex

You need to [extract the server token][plex-token] from a configuration file and
use it as the API key.

#### Plex via Tautulli

Log in and go to `Settings` -> `Web Interface` -> API. Copy the key and make
sure `Enable API` is ticked.


### Multiple Sonarr instances

Configure one `[[sonarr]]` table per instance. When a session starts,
_prefetcharr_ looks for the series on every instance that serves the session's
library and uses the first one that has it.

Give an instance a `libraries` list to route sessions explicitly — for example a
`TV Shows` library to your main Sonarr and an `Anime` library to a dedicated
one. An instance without `libraries` accepts sessions from any library. Note
that `media_server.libraries`, if set, still filters sessions before routing, so
every library you want handled has to be listed there too.

A single `[sonarr]` table remains valid and behaves exactly as before.

### Per-user quality boost

By default _prefetcharr_ only fetches episodes that are missing entirely. A
`[[sonarr.boost]]` rule extends that for the users it names: when one of them
starts watching, the series is switched to the configured quality profile,
tagged, and the next `prefetch_num` episodes are searched individually —
including episodes that are already on disk, so they get *upgraded* rather than
just fetched. Everyone else keeps the default behaviour.

Points worth knowing before enabling it:

- **The profile must allow upgrades.** In _Sonarr_, the quality profile needs
  `Upgrades Allowed` ticked and a cutoff above what your library normally holds.
  Otherwise Sonarr decides existing files are already good enough and nothing
  happens. _prefetcharr_ logs a warning if the profile has upgrades disabled.
- **Custom format score is a separate cutoff.** `Upgrade Until Custom Format
  Score` is independent of the quality cutoff, so an episode can be
  quality-cutoff-met and still far below the target. With a
  [TRaSH][custom-format]/Recyclarr profile this is usually what actually drives
  the upgrades.
- **Reverting is manual.** Boosted series are tagged (`prefetcharr-boosted` by
  default, created in Sonarr if missing). To undo a boost, filter by that tag in
  Sonarr's series editor and change the profile back in bulk. _prefetcharr_
  never reverts a profile on its own.
- **The profile applies to the whole series, for everyone.** Sonarr quality
  profiles are per series, so a boost also affects other users watching it, and
  Sonarr will upgrade the rest of that series on its own schedule.
- **Boosted series are never season-searched.** Sonarr replaces files in place
  when upgrading, and a season pack covers the episode currently being streamed.
  Once a series carries the boost tag, _prefetcharr_ searches individual
  episodes for it even with `request_seasons = true`.
- **Mind your indexer budget.** Each playback event costs up to `prefetch_num`
  searches. Episodes that are already at the target quality, already in the
  download queue, not yet aired, or searched within `search_cooldown` are
  skipped, which keeps repeat triggers cheap.

Set `dry_run = true` to see exactly what a rule would do — every change to
Sonarr is logged and none is applied.

### Retrying unavailable episodes

`retry_unavailable` re-searches episodes that were requested but never turned
up. Retries happen only while the series is still being watched, and at most
once per `retry_interval` per episode, so indexers are not hammered on behalf of
a release that does not exist.

If you stop watching while episodes are still missing, _prefetcharr_ forgets
that it handled that episode, so playing it again another day starts a fresh
prefetch rather than being skipped as already done.

Set `retry_unavailable = false` to keep the original behaviour of searching
exactly once and leaving everything else to _Sonarr_.

### Upgrading pilots

If you want to store pilot episodes only, _prefetcharr_ can fetch the first
season for you on demand.  
This method works well for individual episodes but may encounter issues with
season packs.  
For this to function in _Sonarr_, grabbing the season pack must be considered
an upgrade of the pilot episode.
This can be achieved through a [custom format][custom-format].
[Import][format-import] the custom format and
[configure a quality profile][quality-profile] to prefer it.

## How to use

### Host installation

If you installed _prefetcharr_ through `cargo`, you can get a description of the
command-line interface by running `prefetcharr --help`.

### Docker installation

Users utilizing Docker only need to start the container, e.g. using `docker
compose up -d prefetcharr`.
Once the container is running, you may want to check the logs for errors. You
can do so by either calling `docker logs prefetcharr` or by checking the logging
directory you configured.


[sonarr]: <https://sonarr.tv>
[jellyfin]: <https://jellyfin.org>
[emby]: <https://emby.media>
[plex]: <https://www.plex.tv>
[tautulli]: <https://tautulli.com/>
[rust]: <https://www.rust-lang.org/tools/install>
[toml]: <https://toml.io/en/>
[plex-token]: <https://www.plexopedia.com/plex-media-server/general/plex-token/#plexservertoken>
[custom-format]: <https://trash-guides.info/Sonarr/sonarr-collection-of-custom-formats/#season-pack>
[format-import]: <https://trash-guides.info/Sonarr/sonarr-import-custom-formats/>
[quality-profile]: <https://trash-guides.info/Sonarr/sonarr-setup-quality-profiles/>
