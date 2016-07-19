var CommentForm = React.createClass({
  render: function() {
    return (
      <div className="commentForm"> Hello, world! I am a CommentForm.</div>
    );
  }
});

// tutorial3.js
var CommentBox = React.createClass({
  render: function() {
    return (
      <div className="commentBox">        <h1>Comments</h1>        <CommentList />        <CommentForm />      </div>
    );
  }
});

// tutorial4.js
var Comment = React.createClass({
  render: function() {
    return (
      <div className="comment"><h2 className="commentAuthor">{this.props.author}</h2>{this.props.children}</div>
    );
  }
});

// tutorial5.js
var CommentList = React.createClass({
  render: function() {
    return (
      <div className="commentList">
        <Comment author="Pete Hunt">This is one comment</Comment>
        <Comment author="Jordan Walke">This is *another* comment</Comment>
        </div>
    );
  }
});

ReactDOM.render(
  <CommentList/>,
  document.getElementById('content')
);