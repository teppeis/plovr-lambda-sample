goog.provide('app.success');
goog.require('app.foo');

/**
 * @param {string} foo
 * @return {string}
 */
function getFoo(foo) {
  return foo;
}

console.log(getFoo(app.foo()));
