# PLY Quick Sync

![PLY Quick Sync button](./media/New_Icon.jpg)

`PLY Quick Sync` is a lightweight plugin for [LichtFeld Studio](https://lichtfeld.io/) that adds a one-click toolbar button for quickly overwriting linked `.ply` files.

It is intended for workflows where you want to keep editing in LichtFeld Studio and quickly write changes back to the same `.ply` on disk.

## Features

- one-click quick sync from the top toolbar
- per-node `.ply` overwrite
- multi-selection support without merging models into one file
- explicit node-to-file linking

## Warning

When you run `Quick Sync`, the linked `.ply` file is overwritten in place.

If you do not want to lose the original source file, work on a copy or keep a backup before syncing.

## Installation (LichtFeld Studio v0.5+)

In LichtFeld Studio:

1. Open the `Plugins` panel.
2. Enter:

```text
https://github.com/OlstFlow/Lichtfeld-PLY-Quick_Sync-Plugin
```

3. Install the plugin.
4. Restart LichtFeld Studio if needed.

## Usage

1. Open a `.ply` scene in LichtFeld Studio.
2. Select one or more splat nodes.
3. Link the selected node to a target `.ply` file if needed.
4. Click the `Quick Sync` button in the top toolbar.

If multiple linked nodes are selected, each node is synced to its own linked `.ply` file separately.

## Notes

- This plugin is independent from the Blender addon.
- If you need the Blender-oriented workflow, use the `GauSpla` addon together with the `GauSpla Blender Sync` plugin.
