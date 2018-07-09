goog.provide('app');
goog.require('app.foo');

/**
 * @param {number} foo
 * @return {number}
 */
function getFoo(foo) {
  return foo;
}

console.log(getFoo(app.foo()));
