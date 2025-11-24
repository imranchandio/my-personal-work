# my-personal-work
My Personal Repo
P6_PDS_to_OCI_ADW


ocid : ocid1.dataflowrun.oc1.me-jeddah-1.anvgkljrsvwgetya2dn4q2o3mtrpohb76tp6dhygtuxtncvyctd24bj535tq

ocid1.dataflowapplication.oc1.me-jeddah-1.anvgkljrsvwgetyaw7iyguspejmebw6yafzn4ss7yckzvfohhqwze7rnrc7q
Compartment ocid : ocid1.compartment.oc1..aaaaaaaaly6cyhdxvvnldnu5hfk53hqxxedwc5kxh5ailrbtsmgtt3apguja
application name : P6_PDS_to_OCI_ADW



oci os object get \
  -ns <REAL_NAMESPACE> \
  -bn bkt-neom-enowa-des-dev-data-landing \
  --name common/config/p6/env_variables.csv \
  --query 'data."content"' \
  --raw-output

