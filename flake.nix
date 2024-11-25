{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
    rust-overlay,
  }:
    flake-utils.lib.eachDefaultSystem (system: let
      overlays = [(import rust-overlay)];
      pkgs = import nixpkgs {inherit system overlays;};
    in
      with pkgs; {
        devShell = mkShell {
          buildInputs = with pkgs; [
            pkg-config
            rust-analyzer
            rust-bin.stable.latest.default
          ];

          shellHook = ''
            export OPENSSL_DIR="${pkgs.openssl.dev}"
            export OPENSSL_NO_VENDOR=1
            export OPENSSL_LIB_DIR="${pkgs.lib.getLib pkgs.openssl}/lib"
            export PKG_CONFIG_PATH="${pkgs.openssl.dev}/lib/pkgconfig"
          '';
        };
      });
}
