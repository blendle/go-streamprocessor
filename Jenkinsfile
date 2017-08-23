happyHackin {
  image       = "eu.gcr.io/bnl-blendle/go-streamprocessor:ci"
  buildImages = [[file: 'Dockerfile', tag: 'ci', onBranch: 'master']]

  services = [
    [name: "zookeeper", image: "wurstmeister/zookeeper"],
    [name: "kafka", image: "spotify/kafka", envVars: [containerEnvVar(key: 'KAFKA_DELETE_TOPIC_ENABLE', value: 'true')]],
  ]
}
