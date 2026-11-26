# Notice and license status

## Overall license: not selected

No license has been chosen for this repository as a whole. Until the owner selects one, default copyright applies to the original material here. Nothing in this repository grants new rights over third-party or private content.

## Third-party material

| Path | Origin | License |
|---|---|---|
| `skills/create-test-recipes/` | Adapted from Lauren Tan (Poteto), pstack `create-verification-skill`, [`cursor/plugins` at `c49b37a2c0e7bede60a71ad7322e3f12c337a3da`](https://github.com/cursor/plugins/tree/c49b37a2c0e7bede60a71ad7322e3f12c337a3da/pstack/skills/create-verification-skill) | MIT, notice retained in [`skills/create-test-recipes/LICENSE`](https://github.com/Pattedyret/patstack/blob/c291cc67607984ca45cb99fceb0492918fa1c17d/skills/create-test-recipes/LICENSE); see [`PROVENANCE.md`](https://github.com/Pattedyret/patstack/blob/c291cc67607984ca45cb99fceb0492918fa1c17d/skills/create-test-recipes/PROVENANCE.md) and [`source.json`](https://github.com/Pattedyret/patstack/blob/c291cc67607984ca45cb99fceb0492918fa1c17d/skills/create-test-recipes/source.json) |
| `skills/ponytail/` | Adapted from [Dietrich Gebert's Ponytail](https://github.com/DietrichGebert/ponytail) | MIT; upstream notice in [`LICENSE.txt`](https://github.com/Pattedyret/patstack/blob/c291cc67607984ca45cb99fceb0492918fa1c17d/skills/ponytail/LICENSE.txt) and origin in [`PROVENANCE.json`](https://github.com/Pattedyret/patstack/blob/c291cc67607984ca45cb99fceb0492918fa1c17d/skills/ponytail/PROVENANCE.json) |
| `skills/reverse-engineer-anything/` | Adapted from morluto's [REA](https://github.com/morluto/rea), `.agents/skills/reverse-engineer-anything` at [`17f704050f431c195ba2280aca162e698aea408a`](https://github.com/morluto/rea/tree/17f704050f431c195ba2280aca162e698aea408a/.agents/skills/reverse-engineer-anything). The six reference files are unchanged. `SKILL.md` adds a `tool_count` metadata line and pins `npx -y rea-agents@latest` to `@6.4.0`. | MIT; the upstream notice is reproduced [below](#rea-mit-license) |

## Design sources (read, not copied)

The Patstack workflow was designed after reading the projects listed in [`skills/patstack/references/sources.md`](https://github.com/Pattedyret/patstack/blob/c291cc67607984ca45cb99fceb0492918fa1c17d/skills/patstack/references/sources.md): Theo's tstack, Matt Pocock's skills, Poteto's pstack, Zeroshot's review loop and Ponytail. The adapted material listed above keeps its own license status; the remaining names and links acknowledge design sources, not endorsement. No blanket MIT license is applied to copied third-party material.

## REA MIT license

The notice that ships with REA at the pinned commit above:

```text
MIT License

Copyright (c) 2026 morluto

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

## Product names

Claude, Codex, T3 Code, GitHub, Bitwarden, Playwright and other product names belong to their owners and are used only to describe compatibility.
