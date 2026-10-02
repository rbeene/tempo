# Theme palette sources and color policy

This documents the palette foundation for the planned theme controls. The
generated CLI schema remains the authority for currently available commands.

The embedded catalog contains Terminal default, Tokyo Night, Gruvbox (Material
dark), and Catppuccin Mocha. [Third-party notices](../THIRD_PARTY_NOTICES.md)
identify immutable palette sources, semantic adaptations and complete licenses.
The palette data is built in; loading it requires no network or preference read.

Terminal default inherits colors. Named palettes assign text, secondary text,
border, selection, accent, success, warning, error, contextual information and
keyboard-hint roles consistently. Dark decorative border colors are not used
for meaningful secondary text. Selection uses the readable normal foreground.
Text labels and selection markers must remain legible when colors are disabled.

Capability selection uses the output stream's TTY status and explicit TERM,
COLORTERM and NO_COLOR values. Non-TTY output or any nonempty NO_COLOR disables
color. Unknown, empty and dumb TERM values also disable color, even with a
truecolor hint. Empty NO_COLOR does not disable colors. There is no emulator
query, subprocess discovery, palette-setting escape sequence or prefix guess.

| Recognized TERM | Without a truecolor hint |
|---|---|
| ansi, linux, xterm, xterm-color, screen, tmux, rxvt, rxvt-unicode | ANSI 16 colors |
| xterm-256color, screen-256color, tmux-256color, rxvt-unicode-256color | ANSI 256 colors |
| alacritty, xterm-kitty, xterm-ghostty, foot, foot-extra, wezterm | ANSI 256 colors |

For these exact names only, COLORTERM equal to truecolor or 24bit
(case-insensitive) selects truecolor. Unsupported names still fall back to no
color. These are conservative presentation choices based on supplied evidence,
not a claim that the environment always describes the physical terminal.

The additional modern names are supported by their upstream definitions:
[Alacritty](https://github.com/alacritty/alacritty/blob/master/extra/alacritty.info),
[Kitty](https://github.com/kovidgoyal/kitty/blob/master/terminfo/kitty.terminfo),
[Ghostty](https://github.com/ghostty-org/ghostty/blob/main/src/terminfo/ghostty.zig),
[Foot](https://codeberg.org/dnkl/foot/src/branch/master/foot.info), and
[WezTerm](https://github.com/wezterm/wezterm/blob/main/termwiz/data/wezterm.terminfo).
Foot's [installation guide](https://codeberg.org/dnkl/foot/src/branch/master/INSTALL.md)
documents the foot-extra terminfo name. All these normal entries describe
256-color palettes; explicit truecolor hints enable RGB output in Tempo.

Machine JSON must bypass color styling, including legacy pretty JSON and error
envelopes. A theme command may read preferences to return theme data; other JSON
output must not load appearance preferences merely for presentation. Neither
selecting a theme nor resolving capabilities changes terminal-emulator settings,
activity state, authentication or synchronization.
