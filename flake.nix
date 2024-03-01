{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
  };

  outputs = {
    self,
    nixpkgs,
  }: let
    system = "x86_64-linux";
    pkgs = import nixpkgs {inherit system;};
  in {
    devShells.${system}.default = pkgs.mkShell {
      packages = [
        pkgs.rustc
        pkgs.clippy
        pkgs.cargo
        pkgs.rustfmt
        pkgs.crate2nix
      ];
    };
  };
}
