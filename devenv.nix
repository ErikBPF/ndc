{ pkgs, ... }:
{
  packages = [ pkgs.git pkgs.just ];

  profiles = {
    duckdb.module = {
      packages = [ pkgs.python312 pkgs.duckdb pkgs.shellcheck pkgs.curl ];
      env.NDC_IN_ENV = "1";
      enterTest = "./ndc/run.sh check";
    };

    spark = {
      extends = [ "duckdb" ];
      module = {
        packages = [ pkgs.jdk17 ];
        env.JAVA_HOME = "${pkgs.jdk17}";
        enterShell = ''
          export SPARK41_BASE="''${SPARK41_BASE:-$HOME/ndc-spark41}"
          export SPARK_HOME="''${SPARK_HOME:-$SPARK41_BASE/spark-4.1.3-bin-hadoop3}"
          export PATH="$SPARK_HOME/bin:$PATH"
        '';
      };
    };

    spark-k8s = {
      extends = [ "spark" ];
      module.packages = [ pkgs.kubectl pkgs.devspace ];
    };
  };
}
