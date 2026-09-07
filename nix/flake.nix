{
  description = "NDC benchmark tools";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/5545adfad2e98de106a5544ca7067e03010410bd";
  outputs = { self, nixpkgs }: let
    system = "x86_64-linux";
    pkgs = import nixpkgs { inherit system; };
  in {
    devShells.${system}.default = pkgs.mkShell {
      packages = with pkgs; [ python312 duckdb jdk17 shellcheck curl gnutar gzip ];
      JAVA_HOME = "${pkgs.jdk17}";
    };
  };
}
