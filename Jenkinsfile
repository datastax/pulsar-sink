#!groovy

def initializeEnvironment() {
  env.GIT_SHA = "${env.GIT_COMMIT.take(7)}"
  env.GITHUB_PROJECT_URL = "https://${GIT_URL.replaceFirst(/(git@|http:\/\/|https:\/\/)/, '').replace(':', '/').replace('.git', '')}"
  env.GITHUB_BRANCH_URL = "${GITHUB_PROJECT_URL}/tree/${env.BRANCH_NAME}"
  env.GITHUB_COMMIT_URL = "${GITHUB_PROJECT_URL}/commit/${env.GIT_COMMIT}"
  env.BLUE_OCEAN_URL = "${JENKINS_URL}/blue/organizations/jenkins/tools%27Fpulsar/detail/${BRANCH_NAME}/${BUILD_NUMBER}"

  env.MAVEN_HOME = "${env.HOME}/.mvn/apache-maven-3.2.5"
  env.PATH = "${env.MAVEN_HOME}/bin:${env.PATH}"

  env.JAVA_HOME = sh(label: 'Get JAVA_HOME', script: '''#!/bin/bash -le
    . ${JABBA_SHELL}
    jabba which ${JABBA_VERSION}''', returnStdout: true).trim()

  sh label: 'Download Apache Cassandra(R) or DataStax Enterprise', script: '''#!/bin/bash -le
    . ${JABBA_SHELL}
    jabba use ${JABBA_VERSION}
    . ${CCM_ENVIRONMENT_SHELL} ${CASSANDRA_VERSION}
  '''

  sh label: 'Display Java and environment information', script: '''#!/bin/bash -le
    # Load CCM environment variables
    set -o allexport
    . ${HOME}/environment.txt
    set +o allexport

    . ${JABBA_SHELL}
    jabba use ${JABBA_VERSION}

    java -version
    mvn -v
    printenv | sort
  '''
}

def buildAndExecuteTests() {
  sh label: 'Build and execute tests', script: '''#!/bin/bash -le
    # Load CCM environment variables
    set -o allexport
    . ${HOME}/environment.txt
    set +o allexport

    . ${JABBA_SHELL}
    jabba use ${JABBA_VERSION}

    if [ "${RUN_LONG_TESTS}" = "true" ]; then
      mavenArgs="$mavenArgs -Pmedium"
    fi
    if [ "${GENERATE_DISTRO}" = "true" ]; then
      mavenArgs="$mavenArgs -Prelease -Dgpg.skip=true"
    else
      mavenArgs="$mavenArgs -Dmaven.javadoc.skip=true"
    fi
    
    mvn verify $mavenArgs -B \
      -Ddsbulk.ccm.CCM_VERSION=${CCM_VERSION} \
      -Ddsbulk.ccm.CCM_IS_DSE=${CCM_IS_DSE} \
      -Ddsbulk.ccm.JAVA_HOME=${CCM_JAVA_HOME} \
      -Ddsbulk.ccm.PATH=${CCM_JAVA_HOME}/bin \
      -Ddsbulk.cloud.PROXY_PATH=${HOME}/proxy \
      -Dmaven.test.failure.ignore=true \
      -Dmax.simulacron.clusters=2 \
      -Dmax.ccm.clusters=1
      
    exit $?
  '''
}

def recordTestResults() {
  junit testResults: '**/target/surefire-reports/TEST-*.xml', allowEmptyResults: false
  junit testResults: '**/target/failsafe-reports/TEST-*.xml', allowEmptyResults: false
}

def recordCodeCoverage() {
  if (env.CASSANDRA_VERSION.startsWith("3.11")) {
    jacoco(
            execPattern: '**/target/**.exec',
            exclusionPattern: '**/generated/**'
    )
  }
}

def recordArtifacts() {
  if (params.GENERATE_DISTRO && env.CASSANDRA_VERSION.startsWith("3.11")) {
    archiveArtifacts artifacts: 'pulsar-dist/target/*.tar.gz', fingerprint: true
  }
}

def notifySlack(status = 'started') {

  if (!params.SLACK_ENABLED) {
    return
  }

  if (status == 'started') {
    if (env.SLACK_START_NOTIFIED == 'true') {
      return
    }
    // Set the global pipeline scoped environment (this is above each matrix)
    env.SLACK_START_NOTIFIED = 'true'
  }

  def event = status
  if (status == 'started') {
    String causes = "${currentBuild.buildCauses}"
    def startedByUser = causes.contains('User')
    def startedByCommit = causes.contains('Branch')
    def startedByTimer = causes.contains('Timer')
    if (startedByUser) {
      event = currentBuild.getBuildCauses('hudson.model.Cause$UserIdCause')[0].shortDescription.toLowerCase()
    } else if (startedByCommit) {
      event = "was triggered on commit"
    } else if (startedByTimer) {
      event = "was triggered by timer"
    }
  } else {
    event = "${status == 'failed' ? status.toUpperCase() : status} after ${currentBuild.durationString - ' and counting'}"
  }

  String buildUrl = env.BLUE_OCEAN_URL == null ?
          "#${env.BUILD_NUMBER}" :
          "<${env.BLUE_OCEAN_URL}|#${env.BUILD_NUMBER}>"

  String branchUrl = env.GITHUB_BRANCH_URL == null ?
          "${env.BRANCH_NAME}" :
          "<${env.GITHUB_BRANCH_URL}|${env.BRANCH_NAME}>"

  String commitUrl = env.GIT_SHA == null ?
          "commit unknown" :
          env.GITHUB_COMMIT_URL == null ?
                  "${env.GIT_SHA}" :
                  "<${env.GITHUB_COMMIT_URL}|${env.GIT_SHA}>"

  String message = "Build ${buildUrl} on branch ${branchUrl} (${commitUrl}) ${event}."

  def color = 'good' // Green
  if (status == 'aborted') {
    color = '808080' // Grey
  } else if (status == 'unstable') {
    color = 'warning' // Orange
  } else if (status == 'failed') {
    color = 'danger' // Red
  }

  slackSend channel: "#pulsar-connector",
            message: "${message}",
            color: "${color}"
}

// branch pattern for cron
// should match 3.x, 4.x, 4.5.x, etc
def branchPatternCron = ~"\\d+(\\.\\d+)*\\.x"

pipeline {
  agent none

  options {
    timeout(time: 2, unit: 'HOURS')
    buildDiscarder(logRotator(artifactNumToKeepStr: '10', // Keep only the last 10 artifacts
                              numToKeepStr: '50'))        // Keep only the last 50 build records
  }

  parameters {
    choice(
      name: 'MATRIX_TYPE',
      choices: ['SINGLE', 'FULL'],
      description: '''<p>The matrix to use</p>
                      <table style="width:100%">
                        <col width="25%">
                        <col width="75%">
                        <tr>
                          <th align="left">Choice</th>
                          <th align="left">Description</th>
                        </tr>
                        <tr>
                          <td><strong>SINGLE</strong></td>
                          <td>Runs the test suite against a single C* backend</td>
                        </tr>
                        <tr>
                          <td><strong>FULL</strong></td>
                          <td>Runs the test suite against the full set of configured C* backends</td>
                        </tr>
                      </table>''')
    booleanParam(
      name: 'RUN_LONG_TESTS',
      defaultValue: false,
      description: 'Flag to determine if long tests should be executed (CCM & Cloud)')
    booleanParam(
      name: 'GENERATE_DISTRO',
      defaultValue: false,
      description: 'Flag to determine if the distribution tarball should be generated')
    booleanParam(
      name: 'SLACK_ENABLED',
      defaultValue: true,
      description: 'Flag to determine if Slack notifications should be sent')
  }

  triggers {
    parameterizedCron(branchPatternCron.matcher(env.BRANCH_NAME).matches() ? """
      # Every weeknight (Monday - Friday) around 5:00 AM
      H 5 * * 1-5 % MATRIX_TYPE=FULL; RUN_LONG_TESTS=true; SLACK_ENABLED=false; GENERATE_DISTRO=true
    """ : "")
  }

  environment {
    OS_VERSION = 'ubuntu/bionic64/java-driver'
    JABBA_SHELL = '/usr/lib/jabba/jabba.sh'
    JABBA_VERSION = '1.8'
    CCM_ENVIRONMENT_SHELL = '/usr/local/bin/ccm_environment.sh'
    // always run long tests when generating the distribution tarball
    RUN_LONG_TESTS = "${params.RUN_LONG_TESTS || params.GENERATE_DISTRO}"
    GENERATE_DISTRO = "${params.GENERATE_DISTRO}"
  }

  stages {
    stage ('Single Job') {
      when {
        beforeAgent true
        allOf {
          expression { params.MATRIX_TYPE == 'SINGLE' }
          not { buildingTag() }
        }
      }
      matrix {
        axes {
          axis {
            name 'CASSANDRA_VERSION'
            values '3.11'
          }
        }
        agent {
          label "${OS_VERSION}"
        }
        stages {
          stage('Initialize Environment') {
            steps {
              initializeEnvironment()
              script {
                currentBuild.displayName = "${env.BRANCH_NAME} - ${env.GIT_SHA}"
              }
              notifySlack()
            }
          }
          stage('Build & Test') {
            steps {
              buildAndExecuteTests()
            }
            post {
              success {
                recordTestResults()
                recordCodeCoverage()
                recordArtifacts()
              }
              unstable {
                recordTestResults()
                recordCodeCoverage()
              }
            }
          }
        }
      }
      post {
        aborted {
          notifySlack('aborted')
        }
        success {
          notifySlack('completed')
        }
        unstable {
          notifySlack('unstable')
        }
        failure {
          notifySlack('failed')
        }
      }
    }
    stage('Full Matrix') {
      when {
        beforeAgent true
        allOf {
          expression { params.MATRIX_TYPE == 'FULL' }
          not { buildingTag() }
        }
      }
      matrix {
        axes {
          axis {
            name 'CASSANDRA_VERSION'
            values '3.0',      // Previous Apache Cassandra
                   '3.11',     // Current Apache Cassandra
                   '4.0',      // Development Apache Cassandra
                   'dse-5.1',  // Legacy DataStax Enterprise
                   'dse-6.0',  // Previous DataStax Enterprise
                   'dse-6.7',  // Previous DataStax Enterprise
                   'dse-6.8'   // Current DataStax Enterprise
          }
        }
        agent {
          label "${env.OS_VERSION}"
        }
        stages {
          stage('Initialize Environment') {
            steps {
              initializeEnvironment()
              script {
                currentBuild.displayName = "${env.BRANCH_NAME} - ${env.GIT_SHA} (full)"
              }
              notifySlack()
            }
          }
          stage('Build & Test') {
            steps {
              buildAndExecuteTests()
            }
            post {
              success {
                recordTestResults()
                recordCodeCoverage()
                recordArtifacts()
              }
              unstable {
                recordTestResults()
                recordCodeCoverage()
              }
            }
          }
        }
      }
      post {
        aborted {
          notifySlack('aborted')
        }
        success {
          notifySlack('completed')
        }
        unstable {
          notifySlack('unstable')
        }
        failure {
          notifySlack('failed')
        }
      }
    }
  }
}
