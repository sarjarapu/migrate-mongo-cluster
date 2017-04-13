mongo --port 18000 <<EOF
  use social
  db.dropDatabase()

  sh.enableSharding('social');
  sh.shardCollection('social.people', {_id: 1});
  
  function getRandomString() {
    var value='';
    for(var i=1; i < Math.floor(1+Math.random()*50); i++) {
      value += Math.random().toString(36).substring(2,11);
    }
    return value;
  }

  var users = [];
  for(var i=1; i <= 100; i++) {
    users.push({
      name: 'user'+i, 
      data: getRandomString()
    });
  }
  db.user_archive.insertMany(users);
  db.user.insertMany(users);
  db.user.createIndex({name: 1})

  for(var i=1; i <= 100000; i++) {
    db.people.insert({
      fname: 'fname'+i, 
      lname: 'lname'+i, 
      user: 'user' + Math.floor(Math.random()*100),
      data: getRandomString()
    });
  }
EOF
