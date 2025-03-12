<?php
/**
 * Class: WordPress Term Transformation Pipeline
 *
 * Handles transforming data into WordPress terms with associated metadata.
 *
 * @package TenupETL\Classes\Transform\TransformationPipelines
 */

namespace TenupETL\Classes\Transform\TransformationPipelines;

use Flow\ETL\DataFrame;
use Flow\ETL\Row;

use function TenupETL\Classes\Transform\Transformers\{simple_transformer};
use function Flow\ETL\DSL\{ref, cast, equal, when, lit, entry, now };

/**
 * Pipeline for transforming data into WordPress terms.
 *
 * Provides methods for setting term data like name, slug, parent etc.
 * and handles term metadata and relationships.
 */
class WordPressTermPipeline extends BaseTransformationPipeline {

	/**
	 * Taxonomy for the term.
	 *
	 * @var string
	 */
	protected $taxonomy = 'category';

	/**
	 * Default values for term fields.
	 *
	 * @var array
	 */
	public $defaults = [
		'term_name'   => '',
		'slug'        => '',
		'parent'      => 0,
		'description' => '',
		'alias_of'    => '',
	];

	/**
	 * Prefix for term fields.
	 *
	 * @var string
	 */
	protected $term_prefix = 'term';

	/**
	 * Prefix for ledger fields.
	 *
	 * @var string
	 */
	protected $ledger_prefix = 'ledger';

	/**
	 * Prefix for meta fields.
	 *
	 * @var string
	 */
	protected $meta_prefix = 'meta';

	/**
	 * Get prefixed term field name.
	 *
	 * @param string $term_field The term field.
	 * @return string The prefixed term field name.
	 */
	public function term( string $term_field ) {
		return $this->term_prefix . '.' . $term_field;
	}

	/**
	 * Get prefixed meta field name.
	 *
	 * @param string $meta_field The meta field name.
	 * @return string The prefixed meta field name.
	 */
	public function meta( string $meta_field ) {
		return $this->meta_prefix . '.' . $meta_field;
	}

	/**
	 * Get prefixed ledger field name.
	 *
	 * @param string $column The ledger field name.
	 * @return string The prefixed ledger field name.
	 */
	public function ledger( string $column ) {
		return $this->ledger_prefix . '.' . $column;
	}

	/**
	 * Run the transformation pipeline.
	 *
	 * Sets up all term data fields in sequence.
	 *
	 * @return TransformationPipeline The pipeline instance.
	 */
	public function run(): TransformationPipeline {
		$this
			->prepare()
			->term_taxonomy()
			->term_name()
			->term_slug()
			->term_parent()
			->term_description()
			->term_alias_of()
			->term_meta()
			->ledger_records();

		return $this;
	}

	/**
	 * Set the term taxonomy.
	 *
	 * @return TransformationPipeline The pipeline instance.
	 */
	public function term_taxonomy(): TransformationPipeline {
		$this->state
			->withEntry( $this->term( 'taxonomy' ), lit( $this->taxonomy ) );

		return $this;
	}

	/**
	 * Get default value for a field.
	 *
	 * @param string $key The field key.
	 * @return mixed The default value wrapped in lit().
	 */
	protected function get_default( string $key ): mixed {
		return $this->defaults[ $key ] ? lit( $this->defaults[ $key ] ) : lit( '' );
	}

	/**
	 * Set the term name.
	 *
	 * @return TransformationPipeline The pipeline instance.
	 */
	protected function term_name(): TransformationPipeline {
		return $this;
	}

	/**
	 * Set the term slug.
	 *
	 * @return TransformationPipeline The pipeline instance.
	 */
	protected function term_slug(): TransformationPipeline {
		return $this;
	}

	/**
	 * Set the term parent.
	 *
	 * @return TransformationPipeline The pipeline instance.
	 */
	protected function term_parent(): TransformationPipeline {
		return $this;
	}

	/**
	 * Set the term description.
	 *
	 * @return TransformationPipeline The pipeline instance.
	 */
	protected function term_description(): TransformationPipeline {
		return $this;
	}

	/**
	 * Set the term alias.
	 *
	 * @return TransformationPipeline The pipeline instance.
	 */
	protected function term_alias_of(): TransformationPipeline {
		return $this;
	}

	/**
	 * Set ledger records.
	 *
	 * @return TransformationPipeline The pipeline instance.
	 */
	protected function ledger_records(): TransformationPipeline {
		return $this;
	}
}
