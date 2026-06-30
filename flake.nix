{
  description = "dbus-mqtt-devices (freakent) — transparent-projection dev shell";

  inputs = {
    # Pinned to the same rev hypnos uses, for a guaranteed binary-cache hit.
    nixpkgs.url = "github:NixOS/nixpkgs/567a49d1913ce81ac6e9582e3553dd90a955875f";
    velib_python = {
      url = "github:victronenergy/velib_python";
      flake = false;
    };
  };

  outputs = { self, nixpkgs, velib_python }:
    let
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackages.${system};
      py = pkgs.python3.withPackages (ps: with ps; [
        dbus-python
        pygobject3
        pyyaml
        paho-mqtt
      ]);
    in {
      devShells.${system}.default = pkgs.mkShell {
        packages = [ py pkgs.dbus pkgs.glib pkgs.mosquitto ];
        shellHook = ''
          # velib_python (vedbus.py, settingsdevice.py, ve_utils.py, logger.py) on the path,
          # mirroring the GX layout where the code expects ext/velib_python.
          export VELIB_PYTHON=${velib_python}
          export PYTHONPATH=${velib_python}''${PYTHONPATH:+:}''${PYTHONPATH:-}
        '';
      };
    };
}
