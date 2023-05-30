def gitBranch = env.BRANCH_NAME
def gitURL = "git@github.com:Memphisdev/memphis.py.git"
def repoUrlPrefix = "memphisos"

node ("small-ec2-fleet") {
  git credentialsId: 'main-github', url: gitURL, branch: gitBranch
  
  try{
    
   stage('Deploy to pypi') {
     sh """
       python3 setup.py sdist
       pip3 install twine
       python3 -m pip install urllib3==1.26.6
     """
     withCredentials([usernamePassword(credentialsId: 'python_sdk', usernameVariable: 'USR', passwordVariable: 'PSW')]) {
     sh '/home/ec2-user/.local/bin/twine upload -u $USR -p $PSW dist/*'
    }
   }
    
    stage('Checkout to version branch'){
      sh """
	sudo yum-config-manager --add-repo https://cli.github.com/packages/rpm/gh-cli.repo 
        sudo yum install gh -y
        sed -i -r "s/version=\\"[0-9].[0-9].[0-9]/version=\\"\$(cat version.conf)/g" setup.py
        sed -i -r "s/[0-9].[0-9].[0-9].tar.gz/\$(cat version.conf).tar.gz/g" setup.py
      """
      withCredentials([sshUserPrivateKey(keyFileVariable:'check',credentialsId: 'main-github')]) {
        sh """
	  git reset --hard origin/latest
          GIT_SSH_COMMAND='ssh -i $check' git checkout -b \$(cat version.conf)
          GIT_SSH_COMMAND='ssh -i $check' git push --set-upstream origin \$(cat version.conf)
	"""
      }
      withCredentials([string(credentialsId: 'gh_token', variable: 'GH_TOKEN')]) {
        sh(script:"""gh release create \$(cat version.conf) --generate-notes""", returnStdout: true)
      }
    }


    notifySuccessful()

  } catch (e) {
      currentBuild.result = "FAILED"
      cleanWs()
      notifyFailed()
      throw e
  }
}

def notifySuccessful() {
  emailext (
      subject: "SUCCESSFUL: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]'",
      body: """<p>SUCCESSFUL: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]':</p>
        <p>Check console output at &QUOT;<a href='${env.BUILD_URL}'>${env.JOB_NAME} [${env.BUILD_NUMBER}]</a>&QUOT;</p>""",
      recipientProviders: [[$class: 'DevelopersRecipientProvider']]
    )
}

def notifyFailed() {
  emailext (
      subject: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]'",
      body: """<p>FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]':</p>
        <p>Check console output at &QUOT;<a href='${env.BUILD_URL}'>${env.JOB_NAME} [${env.BUILD_NUMBER}]</a>&QUOT;</p>""",
      recipientProviders: [[$class: 'DevelopersRecipientProvider']]
    )
}
